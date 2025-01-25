import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import telebot
from telebot.async_telebot import AsyncTeleBot
from tjk_utils.prep_env import get_telegram_token, get_telegram_chatId

class TelegramConfig:
    def __init__(self):

        # Initialize monitor as None - will be set later
        self.monitor = None
                    
        # Update frequencies (in minutes)
        self.update_frequencies = {
            'market_snapshot': 60,  # hourly market updates
            'position_update': 60,  # hourly position updates
            'risk_update': 240,     # 4-hourly risk updates
            'performance_update': 360  # 6-hourly performance updates
        }
        
        # Only send these updates during active market hours (8:00-24:00 UTC)
        self.market_hours = {
            'start': 8,  # 8:00 UTC
            'end': 24    # 24:00 UTC
        }
        
        # Message throttling
        self.min_time_between_messages = 60  # seconds
        self.last_message_time = {}
        
        # Priority levels
        self.priorities = {
            'signal': 1,        # Highest priority - always send
            'error': 1,
            'position': 2,      # High priority
            'risk_alert': 2,
            'market': 3,        # Medium priority
            'performance': 4    # Low priority - can be delayed
        }


    def _record_heartbeat(self, component: str):
            """Safely record a heartbeat."""
            if hasattr(self, 'monitor') and self.monitor is not None:
                self.monitor.record_heartbeat(component)
    
    def should_send_update(self, update_type: str, last_update: datetime) -> bool:
        """Check if an update should be sent based on frequency and market hours."""
        now = datetime.now()
        
        # Always send high-priority updates
        if update_type in ['signal', 'error']:
            return True
            
        # Check market hours for non-critical updates
        current_hour = now.hour
        if (current_hour < self.market_hours['start'] or 
            current_hour >= self.market_hours['end']):
            return False
            
        # Check frequency
        if update_type in self.update_frequencies:
            minutes_since_update = (now - last_update).total_seconds() / 60
            return minutes_since_update >= self.update_frequencies[update_type]
            
        return False
        
    def can_send_message(self, message_type: str) -> bool:
        """Check if a message can be sent based on throttling rules."""
        now = datetime.now()
        
        # Always allow high-priority messages
        if self.priorities.get(message_type, 999) <= 2:
            return True
            
        # Check throttling
        if message_type in self.last_message_time:
            seconds_since_last = (now - self.last_message_time[message_type]).total_seconds()
            if seconds_since_last < self.min_time_between_messages:
                return False
                
        self.last_message_time[message_type] = now
        return True

class TradingSystemReporter:
    """
    Handles reporting of trading system events to Telegram channel using telebot.
    """
    
    def __init__(self, trading_system: 'IntegratedTradingSystem'):
        """
        Initialize the reporter.
        
        Args:
            trading_system: Reference to the main trading system
        """
        self.telegram_config = TelegramConfig()
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        self.trading_system = trading_system
        self.telegram_token = get_telegram_token()
        self.chat_id = get_telegram_chatId()
        
        # Initialize Telegram bot
        self.bot = AsyncTeleBot(self.telegram_token)
        
        # Message formatting settings
        self.emoji_map = {
            'success': '✅',
            'error': '❌',
            'warning': '⚠️',
            'info': 'ℹ️',
            'long': '🟢',
            'short': '🔴',
            'neutral': '⚪',
            'profit': '📈',
            'loss': '📉'
        }
        
        # Reporting settings
        self.reporting_levels = {
            'websocket_status': True,
            'market_snapshots': True,
            'strategy_updates': True,
            'signals': True,
            'trades': True,
            'risk_updates': True
        }

    async def start_reporting(self):
        """Start the reporting system."""
        try:
            # Send initial status message
            await self.send_system_startup_message()
            
            # Start periodic status updates
            asyncio.create_task(self._periodic_status_updates())
            
            self.logger.info("Trading system reporter started")
            
        except Exception as e:
            self.logger.error(f"Error starting reporter: {e}")

    async def send_system_startup_message(self):
        """Send system startup notification."""
        message = f"{self.emoji_map['info']} Trading System Started\n\n"
        message += "Monitoring pairs:\n"
        for pair in self.trading_system.trading_pairs:
            message += f"• {pair}\n"
        
        await self.send_telegram_message(message)

    async def report_websocket_status(self, status: Dict):
        """Report websocket connection status."""
        if not self.reporting_levels['websocket_status']:
            return
            
        try:
            emoji = self.emoji_map['success'] if status['connected'] else self.emoji_map['error']
            message = f"{emoji} WebSocket Status Update\n\n"
            message += f"Connected: {status['connected']}\n"
            message += f"Active Tasks: {status.get('active_tasks', 0)}\n"
            message += "\nSubscribed Pairs:\n"
            
            for pair in status.get('subscribed_pairs', []):
                message += f"• {pair}\n"
            
            await self.send_telegram_message(message)
            
        except Exception as e:
            self.logger.error(f"Error reporting websocket status: {e}")

    async def report_market_snapshot(self, pair: str, snapshot: Dict):
        """Report market snapshot updates."""
        if not self.reporting_levels['market_snapshots']:
            return

        # Track last update time for this pair
        if not hasattr(self, '_last_updates'):
            self._last_updates = {}
            
        last_update = self._last_updates.get(pair, datetime.min)

        if not self.telegram_config.should_send_update('market_snapshot', last_update):
            return

        # Update the last update time
        self._last_updates[pair] = datetime.now()
            
        try:
            message = f"{self.emoji_map['info']} Market Snapshot: {pair}\n\n"
            message += f"Price: ${snapshot['price']:.2f}\n"
            message += f"24h Change: {snapshot['change_24h']:.2f}%\n"
            message += f"Volume: ${snapshot['volume_24h']:,.0f}\n"
            message += f"Volatility: {snapshot['volatility']:.2f}%\n"
            
            await self.send_telegram_message(message)
            
        except Exception as e:
            self.logger.error(f"Error reporting market snapshot: {e}")

    async def report_strategy_signal_old(self, pair: str, signals: Dict):
        """Report strategy signals."""
        if not self.reporting_levels['signals']:
            return
            
        try:
            if any(signals.values()):  # Only report if there are active signals
                message = f"{self.emoji_map['info']} Strategy Signal: {pair}\n\n"
                
                if signals.get('entries_long'):
                    message += f"{self.emoji_map['long']} Long Entry Signal\n"
                if signals.get('entries_short'):
                    message += f"{self.emoji_map['short']} Short Entry Signal\n"
                if signals.get('exits_long'):
                    message += f"{self.emoji_map['neutral']} Long Exit Signal\n"
                if signals.get('exits_short'):
                    message += f"{self.emoji_map['neutral']} Short Exit Signal\n"
                
                await self.send_telegram_message(message)
                
        except Exception as e:
            self.logger.error(f"Error reporting strategy signal: {e}")

    async def report_strategy_signal(self, pair: str, signals: Dict):
        """Report strategy signals."""
        if not self.reporting_levels['signals']:
            return
            
        try:
            # Check if there are any active signals
            has_signals = False
            message = f"{self.emoji_map['info']} Strategy Signal: {pair}\n\n"
            
            # Check each signal type safely
            if signals.get('entries_long') is not None and signals['entries_long'].iloc[-1]:
                message += f"{self.emoji_map['long']} Long Entry Signal\n"
                has_signals = True
                
            if signals.get('entries_short') is not None and signals['entries_short'].iloc[-1]:
                message += f"{self.emoji_map['short']} Short Entry Signal\n"
                has_signals = True
                
            if signals.get('exits_long') is not None and signals['exits_long'].iloc[-1]:
                message += f"{self.emoji_map['neutral']} Long Exit Signal\n"
                has_signals = True
                
            if signals.get('exits_short') is not None and signals['exits_short'].iloc[-1]:
                message += f"{self.emoji_map['neutral']} Short Exit Signal\n"
                has_signals = True
            
            # Only send message if there are active signals
            if has_signals:
                await self.send_telegram_message(message)
                
        except Exception as e:
            self.logger.error(f"Error reporting strategy signal: {e}")
    
    async def report_trade_execution(self, trade: Dict):
        """Report trade execution details."""
        if not self.reporting_levels['trades']:
            return
            
        try:
            side_emoji = self.emoji_map['long'] if trade['side'] == 'BUY' else self.emoji_map['short']
            message = f"{side_emoji} Trade Executed: {trade['symbol']}\n\n"
            message += f"Side: {trade['side']}\n"
            message += f"Price: ${float(trade['price']):.2f}\n"
            message += f"Quantity: {float(trade['quantity']):.8f}\n"
            message += f"Value: ${float(trade['price']) * float(trade['quantity']):.2f}\n"
            
            await self.send_telegram_message(message)
            
        except Exception as e:
            self.logger.error(f"Error reporting trade execution: {e}")

    async def report_risk_update(self, metrics: Dict):
        """Report risk metric updates."""
        if not self.reporting_levels['risk_updates']:
            return
            
        try:
            risk_level = metrics['risk_metrics']['risk_level']
            emoji = self.emoji_map['warning'] if risk_level in ['high', 'very_high'] else self.emoji_map['info']
            
            message = f"{emoji} Risk Update\n\n"
            message += f"Risk Level: {risk_level}\n"
            message += f"Exposure: {metrics['exposure_metrics']['gross_exposure']:.2f}%\n"
            message += f"Largest Position: {metrics['risk_metrics']['largest_position_pct']:.2f}%\n"
            
            await self.send_telegram_message(message)
            
        except Exception as e:
            self.logger.error(f"Error reporting risk update: {e}")

    async def report_performance_summary(self, summary: Dict):
        """Report periodic performance summary."""
        try:
            pnl = summary['trading_performance']['total_pnl']
            emoji = self.emoji_map['profit'] if pnl >= 0 else self.emoji_map['loss']
            
            message = f"{emoji} Performance Summary\n\n"
            message += f"Total PnL: ${pnl:.2f}\n"
            message += f"Total Trades: {summary['trading_performance']['total_trades']}\n"
            message += f"Win Rate: {summary['trading_performance']['winning_trades'] / max(1, summary['trading_performance']['total_trades']) * 100:.1f}%\n"
            
            await self.send_telegram_message(message)
            
        except Exception as e:
            self.logger.error(f"Error reporting performance summary: {e}")

    async def report_error(self, error_type: str, error_message: str):
        """Report system errors."""
        try:
            message = f"{self.emoji_map['error']} System Error\n\n"
            message += f"Type: {error_type}\n"
            message += f"Message: {error_message}\n"
            
            await self.send_telegram_message(message)
            
        except Exception as e:
            self.logger.error(f"Error reporting system error: {e}")

    async def _periodic_status_updates(self):
        """Send periodic status updates."""
        while True:
            try:
                # Get system status
                status = self.trading_system.get_system_status()
                
                # Report websocket status
                await self.report_websocket_status(status['data_manager_status'])
                
                # Report performance
                performance = self.trading_system.get_performance_summary()
                await self.report_performance_summary(performance)
                
                # Wait for next update interval (e.g., every hour)
                await asyncio.sleep(3600)
                
            except Exception as e:
                self.logger.error(f"Error in periodic status updates: {e}")
                await asyncio.sleep(60)

    async def send_telegram_message(self, message: str):
        """Send message to Telegram channel with retry logic."""
        max_retries = 3
        retry_delay = 5  # seconds
        
        for attempt in range(max_retries):
            try:
                await self.bot.send_message(
                    chat_id=self.chat_id,
                    text=message,
                    parse_mode='HTML'
                )
                return
            except Exception as e:
                if attempt == max_retries - 1:  # Last attempt
                    self.logger.error(f"Failed to send Telegram message after {max_retries} attempts: {e}")
                else:
                    await asyncio.sleep(retry_delay * (attempt + 1))

# Usage example:
# reporter = TradingSystemReporter(trading_system)
# await reporter.start_reporting()