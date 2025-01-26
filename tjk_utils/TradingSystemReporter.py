import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from collections import deque
import json
import pytz
from telebot.async_telebot import AsyncTeleBot
from tjk_utils.position_market_reporter import PositionMarketReporter

class MessagePriority(Enum):
    CRITICAL = 0    # System errors, critical alerts
    HIGH = 1        # Trade signals, position updates
    MEDIUM = 2      # Risk updates, performance metrics
    LOW = 3         # Regular status updates, market info

@dataclass
class QueuedMessage:
    priority: MessagePriority
    content: str
    timestamp: datetime
    attempts: int = 0
    max_attempts: int = 3
    tags: List[str] = None

    def __lt__(self, other):
        return self.priority.value < other.priority.value

class MessageQueue:
    def __init__(self, max_size: int = 1000):
        self.queue = deque(maxlen=max_size)
        self.processing = False
        self._lock = asyncio.Lock()

    async def put(self, message: QueuedMessage):
        async with self._lock:
            self.queue.append(message)
            # Sort by priority (lower value = higher priority)
            sorted_queue = sorted(self.queue)
            self.queue.clear()
            self.queue.extend(sorted_queue)

    async def get(self) -> Optional[QueuedMessage]:
        async with self._lock:
            return self.queue.popleft() if self.queue else None

    def is_empty(self) -> bool:
        return len(self.queue) == 0

class TelegramMessageHandler:
    def __init__(self, bot_token: str, chat_id: str):
        self.bot = AsyncTeleBot(bot_token)
        self.chat_id = chat_id
        self.logger = logging.getLogger(__name__)
        self.queue = MessageQueue()
        
        # Throttling settings
        self.rate_limits = {
            MessagePriority.CRITICAL: 1,      # 1 second
            MessagePriority.HIGH: 5,          # 5 seconds
            MessagePriority.MEDIUM: 30,       # 30 seconds
            MessagePriority.LOW: 60           # 60 seconds
        }
        self.last_sent = {priority: datetime.min for priority in MessagePriority}
        
        # Error handling
        self.consecutive_failures = 0
        self.max_consecutive_failures = 5
        self.backoff_base = 2
        
        # Monitoring
        self.stats = {
            'messages_sent': 0,
            'messages_failed': 0,
            'last_success': None,
            'last_error': None
        }

    async def start(self):
        """Start the message processing loop."""
        try:
            while True:
                if not self.queue.is_empty():
                    message = await self.queue.get()
                    if message:
                        await self._process_message(message)
                await asyncio.sleep(0.1)
        except Exception as e:
            self.logger.error(f"Error in message processing loop: {e}")
            raise

    async def _process_message(self, message: QueuedMessage):
        """Process a single message with throttling and error handling."""
        try:
            # Check rate limits
            if not self._can_send(message.priority):
                await self.queue.put(message)
                return

            # Attempt to send message
            if await self._send_message(message):
                self.consecutive_failures = 0
                self.stats['messages_sent'] += 1
                self.stats['last_success'] = datetime.now()
                self.last_sent[message.priority] = datetime.now()
            else:
                await self._handle_send_failure(message)

        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            await self._handle_send_failure(message)

    async def _send_message(self, message: QueuedMessage) -> bool:
        """Send a message through Telegram."""
        try:
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message.content,
                parse_mode='HTML'
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            self.stats['messages_failed'] += 1
            self.stats['last_error'] = str(e)
            return False

    async def _handle_send_failure(self, message: QueuedMessage):
        """Handle message sending failures with exponential backoff."""
        self.consecutive_failures += 1
        
        if message.attempts < message.max_attempts:
            message.attempts += 1
            backoff = self.backoff_base ** (message.attempts - 1)
            await asyncio.sleep(backoff)
            await self.queue.put(message)
        else:
            self.logger.error(f"Message failed after {message.max_attempts} attempts: {message.content}")

        if self.consecutive_failures >= self.max_consecutive_failures:
            self.logger.critical("Too many consecutive failures. Initiating reconnection...")
            await self._reconnect()

    def _can_send(self, priority: MessagePriority) -> bool:
        """Check if we can send a message based on rate limits."""
        now = datetime.now()
        last_sent = self.last_sent.get(priority, datetime.min)
        time_since_last = (now - last_sent).total_seconds()
        return time_since_last >= self.rate_limits[priority]

    async def _reconnect(self):
        """Attempt to reconnect the Telegram bot."""
        try:
            self.bot = AsyncTeleBot(self.bot._token)
            self.consecutive_failures = 0
            self.logger.info("Successfully reconnected Telegram bot")
        except Exception as e:
            self.logger.error(f"Failed to reconnect: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get current handler statistics."""
        return {
            **self.stats,
            'queue_size': len(self.queue.queue),
            'consecutive_failures': self.consecutive_failures
        }

    async def enqueue_message(self, content: str, priority: MessagePriority, tags: List[str] = None):
        """Enqueue a new message."""
        message = QueuedMessage(
            priority=priority,
            content=content,
            timestamp=datetime.now(pytz.UTC),
            tags=tags or []
        )
        await self.queue.put(message)

    async def format_and_enqueue(self, 
                               message_type: str,
                               data: Dict[str, Any],
                               priority: MessagePriority = MessagePriority.MEDIUM):
        """Format and enqueue different types of messages."""
        content = self._format_message(message_type, data)
        if content:
            await self.enqueue_message(content, priority, tags=[message_type])

    def _format_message(self, message_type: str, data: Dict[str, Any]) -> Optional[str]:
        """Format different types of messages."""
        try:
            if message_type == "trade_signal":
                return (f"🎯 Trade Signal for {data['symbol']}\n"
                       f"Type: {data['signal_type']}\n"
                       f"Price: ${data['price']:.2f}\n"
                       f"Time: {data['timestamp']}")
                       
            elif message_type == "position_update":
                return (f"📊 Position Update for {data['symbol']}\n"
                       f"Size: {data['size']}\n"
                       f"PnL: ${data['pnl']:.2f}\n"
                       f"ROI: {data['roi']:.2f}%")
                       
            elif message_type == "risk_alert":
                return (f"⚠️ Risk Alert\n"
                       f"Type: {data['alert_type']}\n"
                       f"Details: {data['details']}\n"
                       f"Action Required: {data['action']}")
                       
            elif message_type == "performance_update":
                return (f"📈 Performance Update\n"
                       f"Total PnL: ${data['total_pnl']:.2f}\n"
                       f"Win Rate: {data['win_rate']:.1f}%\n"
                       f"Sharpe Ratio: {data['sharpe']:.2f}")
                       
            elif message_type == "system_status":
                return (f"🖥️ System Status Update\n"
                       f"Status: {data['status']}\n"
                       f"Active Positions: {data['active_positions']}\n"
                       f"CPU Usage: {data['cpu_usage']}%\n"
                       f"Memory Usage: {data['memory_usage']}%")
                       
            elif message_type == "error":
                return (f"🚨 System Error\n"
                       f"Component: {data['component']}\n"
                       f"Error: {data['error']}\n"
                       f"Time: {data['timestamp']}")
                       
            return None
            
        except Exception as e:
            self.logger.error(f"Error formatting message: {e}")
            return None

class TradingSystemReporter:
    """Handles reporting of trading system events to Telegram channel."""
    
    def __init__(self, trading_system: 'IntegratedTradingSystem'):
        self.trading_system = trading_system
        self.logger = logging.getLogger(__name__)
        self.message_handler = None
        self._is_running = False
        
        # Update frequencies (in minutes)
        self.update_frequencies = {
            'market_snapshot': 60,    # Hourly market updates
            'position_update': 60,    # Hourly position updates
            'risk_update': 240,       # 4-hourly risk updates
            'performance_update': 360  # 6-hourly performance updates
        }
        
         # Track last updates with timezone-aware datetime
        self.last_update = {
            key: datetime.now(pytz.UTC) 
            for key in self.update_frequencies
        }
        
        # Initialize monitoring
        self.monitor = None
        
        # Initialize position market reporter
        self.position_market_reporter = PositionMarketReporter()

    async def update_positions_and_markets(self):
        """Send updates for all positions and market conditions."""
        try:
            if not self.message_handler:
                self.logger.error("Message handler not initialized")
                return

            # Get portfolio metrics
            portfolio_metrics = self.trading_system.trader.calculate_portfolio_metrics()
            
            # Send portfolio summary first
            portfolio_message = self.position_market_reporter.format_portfolio_message(
                positions=portfolio_metrics.get('positions', []),
                portfolio_value=portfolio_metrics.get('total_value', 0)
            )
            await self.message_handler.enqueue_message(
                portfolio_message,
                MessagePriority.HIGH
            )
            
            # Send individual position updates
            for position in portfolio_metrics.get('positions', []):
                symbol = position.get('symbol')
                if not symbol:
                    continue
                    
                # Get market data
                market_data = self.trading_system.data_manager.get_market_snapshot(symbol)
                if not market_data:
                    continue
                    
                position_message = self.position_market_reporter.format_position_message(
                    position=position,
                    market_data=market_data
                )
                await self.message_handler.enqueue_message(
                    position_message,
                    MessagePriority.MEDIUM
                )
            
            # Send market snapshots for all pairs
            for symbol in self.trading_system.trading_pairs:
                market_data = self.trading_system.data_manager.get_market_snapshot(symbol)
                order_book = self.trading_system.data_manager.get_order_book_metrics(symbol)
                
                if market_data:
                    market_message = self.position_market_reporter.format_market_message(
                        symbol=symbol,
                        market_data=market_data,
                        order_book=order_book or {}
                    )
                    await self.message_handler.enqueue_message(
                        market_message,
                        MessagePriority.LOW
                    )
            
            # Record heartbeat
            if self.monitor:
                self.monitor.record_heartbeat('telegram_reporter')
                
        except Exception as e:
            self.logger.error(f"Error updating positions and markets: {e}")
            if self.monitor:
                self.monitor.record_heartbeat('telegram_reporter_error')

    async def start_reporting(self):
        """Start the reporting system."""
        try:
            # Initialize telegram handler
            self.message_handler = TelegramMessageHandler(
                bot_token=self.trading_system.telegram_token,
                chat_id=self.trading_system.telegram_chat_id
            )
            
            # Start message handler
            self._is_running = True
            asyncio.create_task(self.message_handler.start())
            
            # Send initial status message
            await self._send_startup_message()
            
            # Start periodic updates
            asyncio.create_task(self._run_periodic_updates())
            
            self.logger.info("Trading system reporter started successfully")
            
        except Exception as e:
            self.logger.error(f"Error starting reporter: {e}")
            raise

    async def stop_reporting(self):
        """Stop the reporting system."""
        self._is_running = False
        self.logger.info("Trading system reporter stopped")

    async def _send_startup_message(self):
        """Send system startup notification."""
        await self.message_handler.enqueue_message(
            "🚀 Trading System Started\n\n"
            f"Monitoring pairs: {', '.join(self.trading_system.trading_pairs)}\n"
            f"Time: {datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}",
            MessagePriority.HIGH
        )

    async def _run_periodic_updates(self):
        """Run periodic status updates."""
        while self._is_running:
            try:
                now = datetime.now(pytz.UTC)
                
                # Check and send updates based on frequencies
                for update_type, frequency in self.update_frequencies.items():
                    last_update = self.last_update[update_type]
                    if (now - last_update).total_seconds() >= frequency * 60:
                        await self._send_update(update_type)
                        self.last_update[update_type] = now
                
                if self.monitor:
                    self.monitor.record_heartbeat('telegram_reporter')
                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error in periodic updates: {e}")
                await asyncio.sleep(5)

    async def _send_update(self, update_type: str):
        """Send a specific type of update."""
        try:
            if update_type == 'market_snapshot':
                await self._send_market_snapshot()
            elif update_type == 'position_update':
                await self._send_position_update()
            elif update_type == 'risk_update':
                await self._send_risk_update()
            elif update_type == 'performance_update':
                await self._send_performance_update()
                
        except Exception as e:
            self.logger.error(f"Error sending {update_type}: {e}")

    async def _send_market_snapshot(self):
        """Send market snapshot for all trading pairs."""
        for pair in self.trading_system.trading_pairs:
            try:
                snapshot = self.trading_system.data_manager.get_market_snapshot(pair)
                if snapshot:
                    await self.message_handler.format_and_enqueue(
                        'market_snapshot',
                        {
                            'symbol': pair,
                            'price': snapshot['price'],
                            'change_24h': snapshot['change_24h'],
                            'volume': snapshot['volume_24h'],
                            'timestamp': datetime.now(pytz.UTC)
                        },
                        MessagePriority.LOW
                    )
            except Exception as e:
                self.logger.error(f"Error getting market snapshot for {pair}: {e}")

    async def _send_position_update(self):
        """Send update on all active positions."""
        try:
            positions = self.trading_system.active_positions
            for symbol, position in positions.items():
                market_data = self.trading_system.data_manager.get_market_snapshot(symbol)
                if market_data:
                    current_price = market_data['price']
                    entry_price = position.get('entry_price', current_price)
                    size = position.get('position_size', 0)
                    pnl = (current_price - entry_price) * size
                    roi = (pnl / (entry_price * size)) * 100 if entry_price * size != 0 else 0
                    
                    await self.message_handler.format_and_enqueue(
                        'position_update',
                        {
                            'symbol': symbol,
                            'size': size,
                            'entry_price': entry_price,
                            'current_price': current_price,
                            'pnl': pnl,
                            'roi': roi,
                            'timestamp': datetime.now(pytz.UTC)
                        },
                        MessagePriority.HIGH
                    )
        except Exception as e:
            self.logger.error(f"Error sending position updates: {e}")

    async def _send_risk_update(self):
        """Send update on current risk metrics."""
        try:
            portfolio_metrics = self.trading_system.trader.calculate_portfolio_metrics()
            risk_metrics = portfolio_metrics.get('risk_metrics', {})
            
            await self.message_handler.format_and_enqueue(
                'risk_alert',
                {
                    'alert_type': 'Risk Metrics Update',
                    'details': {
                        'largest_position': f"{risk_metrics.get('largest_position_pct', 0):.1f}%",
                        'concentration': f"{risk_metrics.get('concentration_score', 0):.2f}",
                        'leverage': f"{risk_metrics.get('leverage_ratio', 1):.2f}x",
                        'risk_level': risk_metrics.get('risk_level', 'unknown')
                    },
                    'action': self._get_risk_action(risk_metrics),
                    'timestamp': datetime.now(pytz.UTC)
                },
                MessagePriority.MEDIUM
            )
            
        except Exception as e:
            self.logger.error(f"Error sending risk update: {e}")

    def _get_risk_action(self, risk_metrics: Dict) -> str:
        """Determine appropriate risk management action."""
        risk_level = risk_metrics.get('risk_level', 'unknown')
        actions = {
            'very_high': "Immediate position reduction required",
            'high': "Consider reducing exposure",
            'medium': "Monitor positions closely",
            'low': "Normal operation - maintain vigilance",
            'very_low': "Consider increasing position sizes",
            'unknown': "Check risk monitoring system"
        }
        return actions.get(risk_level, "Monitor system status")

    async def _send_performance_update(self):
        """Send performance metrics update."""
        try:
            performance = self.trading_system.trader.generate_performance_report()
            
            if performance.get('status') != 'no_trades':
                summary = performance.get('summary', {})
                await self.message_handler.format_and_enqueue(
                    'performance_update',
                    {
                        'total_pnl': summary.get('total_pnl', 0),
                        'win_rate': summary.get('winning_trades', 0) / max(summary.get('total_trades', 1), 1) * 100,
                        'sharpe': performance.get('risk_metrics', {}).get('sharpe_ratio', 0),
                        'timestamp': datetime.now(pytz.UTC)
                    },
                    MessagePriority.MEDIUM
                )
        
        except Exception as e:
            self.logger.error(f"Error sending performance update: {e}")

    async def report_trade_signal(self, symbol: str, signal_type: str, price: float):
        """Report a new trade signal."""
        try:
            await self.message_handler.format_and_enqueue(
                'trade_signal',
                {
                    'symbol': symbol,
                    'signal_type': signal_type,
                    'price': price,
                    'timestamp': datetime.now(pytz.UTC)
                },
                MessagePriority.HIGH
            )
        except Exception as e:
            self.logger.error(f"Error reporting trade signal: {e}")

    async def report_error(self, component: str, error: str):
        """Report system errors."""
        try:
            await self.message_handler.format_and_enqueue(
                'error',
                {
                    'component': component,
                    'error': str(error),
                    'timestamp': datetime.now(pytz.UTC)
                },
                MessagePriority.CRITICAL
            )
        except Exception as e:
            self.logger.error(f"Error reporting system error: {e}")

    async def report_websocket_status(self, status: Dict[str, Any]):
        """Report websocket connection status."""
        try:
            await self.message_handler.format_and_enqueue(
                'system_status',
                {
                    'status': 'WEBSOCKET UPDATE',
                    'connected': status.get('connected', False),
                    'active_pairs': len(status.get('subscribed_pairs', [])),
                    'last_update': status.get('last_update', {}),
                    'cpu_usage': 0,  # Add system metrics if available
                    'memory_usage': 0
                },
                MessagePriority.HIGH if not status.get('connected', False) else MessagePriority.MEDIUM
            )
        except Exception as e:
            self.logger.error(f"Error reporting websocket status: {e}")

    def get_reporter_status(self) -> Dict[str, Any]:
        """Get current reporter status and statistics."""
        try:
            if not self.message_handler:
                return {'status': 'not_initialized'}
                
            stats = self.message_handler.get_stats()
            return {
                'status': 'running' if self._is_running else 'stopped',
                'message_stats': stats,
                'last_updates': {k: v.isoformat() for k, v in self.last_update.items()},
                'monitoring_pairs': len(self.trading_system.trading_pairs)
            }
        except Exception as e:
            self.logger.error(f"Error getting reporter status: {e}")
            return {'status': 'error', 'error': str(e)}

    async def report_positions(self):
        """Report all active positions."""
        try:
            # Get all active positions
            positions = self.trading_system.active_positions
            if not positions:
                await self.message_handler.enqueue_message(
                    "No active positions currently.",
                    MessagePriority.LOW
                )
                return
    
            # Get portfolio metrics
            portfolio_metrics = self.trading_system.trader.calculate_portfolio_metrics()
            total_value = portfolio_metrics.get('total_value', 0)
            daily_pnl = portfolio_metrics.get('pnl_24h', 0)
    
            # First send portfolio summary
            summary = await PositionReporter.format_portfolio_summary(
                positions, total_value, daily_pnl
            )
            await self.message_handler.enqueue_message(
                summary,
                MessagePriority.HIGH
            )
    
            # Then send individual position reports
            for symbol, position in positions.items():
                # Get market data for the position
                market_data = self.trading_system.data_manager.get_market_snapshot(symbol)
                if not market_data:
                    continue
    
                position_report = await PositionReporter.format_position_report(
                    position, market_data
                )
                await self.message_handler.enqueue_message(
                    position_report,
                    MessagePriority.MEDIUM
                )
    
        except Exception as e:
            self.logger.error(f"Error reporting positions: {e}")
            await self.report_error("position_reporter", str(e))
    
    async def report_market_snapshot(self, symbol: str = None):
        """Report market snapshot for one or all symbols."""
        try:
            symbols = [symbol] if symbol else self.trading_system.trading_pairs
            
            for sym in symbols:
                try:
                    # Get market data
                    market_data = self.trading_system.data_manager.get_market_snapshot(sym)
                    if not market_data:
                        continue
    
                    # Get order book metrics
                    order_book = self.trading_system.data_manager.get_order_book_metrics(sym)
                    if not order_book:
                        order_book = {}
    
                    # Format and send snapshot
                    snapshot = await PositionReporter.format_market_snapshot(
                        sym, market_data, order_book
                    )
                    await self.message_handler.enqueue_message(
                        snapshot,
                        MessagePriority.LOW
                    )
    
                except Exception as e:
                    self.logger.error(f"Error reporting market snapshot for {sym}: {e}")
    
        except Exception as e:
            self.logger.error(f"Error in report_market_snapshot: {e}")
            await self.report_error("market_reporter", str(e))
    
    # Usage in the trading system:
    async def report_status(self):
        """Report complete system status."""
        try:
            # First report positions
            await self.report_positions()
            
            # Then report market snapshots
            await self.report_market_snapshot()
            
            # Get system metrics
            metrics = self.trading_system.get_system_status()
            
            # Report system status
            await self.message_handler.enqueue_message(
                f"System Status:\n"
                f"Websocket Connected: {metrics['websocket_connected']}\n"
                f"Active Tasks: {metrics['active_tasks']}\n"
                f"Memory Usage: {metrics['memory_usage']}MB\n"
                f"Last Update: {datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S UTC')}",
                MessagePriority.HIGH
            )
            
        except Exception as e:
            self.logger.error(f"Error reporting system status: {e}")
            await self.report_error("status_reporter", str(e))
