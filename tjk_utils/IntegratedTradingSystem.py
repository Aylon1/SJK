import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
import pandas as pd
import numpy as np
import vectorbtpro as vbt
import pytz
import gc
import psutil

import asyncio
# Import your classes
import sys
import os

# Add project root to path if needed
if '..' not in sys.path:
    sys.path.append('..')

from tjk_utils.TradingManager import BinanceTrader
from tjk_utils.AccountDataManager import BinanceSpotAccount
from tjk_utils.CryptoDataManager import CryptoDataManager
from tjk_utils.TradingSystemReporter import TradingSystemReporter
from tjk_utils.prep_env import (
    get_api_key,
    get_ccxt_exchange,
    get_telegram_token,
    get_telegram_chatId
)
from tjk_utils.strategy import EMACrossStrategy
from tjk_utils.SystemMonitor import SystemMonitor
from tjk_utils.TradingSystemReporter import TradingSystemReporter
from tjk_utils.position_market_reporter import PositionMarketReporter
#from integrated_trading_system import IntegratedTradingSystem

# Configure logging to file only
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../logs/trading_system.log'),
        # Remove StreamHandler to stop console output
    ]
)

# Disable Jupyter notebook output for specific loggers
#logging.getLogger('websockets').setLevel(logging.WARNING)
#logging.getLogger('asyncio').setLevel(logging.WARNING)
#logging.getLogger('ccxt.base.exchange').setLevel(logging.WARNING)

class IntegratedTradingSystem:
    """
    Integrated Trading System that combines data management, strategy execution,
    order management, and system monitoring.
    """

    def __init__(
        self,
        account_manager: 'BinanceSpotAccount',
        data_manager: 'CryptoDataManager',
        trader: 'BinanceTrader',
        trading_pairs: List[str],
        telegram_reporter: Optional['TradingSystemReporter'] = None,
        strategy_update_interval: int = 3600,  # 1 hour
        telegram_update_interval: int = 900,  # 15 minutes
    ):
        """
        Initialize the integrated trading system.

        Args:
            account_manager: BinanceSpotAccount instance.
            data_manager: CryptoDataManager instance.
            trader: BinanceTrader instance.
            trading_pairs: List of trading pairs to monitor.
            telegram_reporter: TradingSystemReporter instance for Telegram updates.
            strategy_update_interval: Strategy update interval in seconds.
            telegram_update_interval: Telegram update interval in seconds.
        """
        # Core components
        self.account = account_manager
        self.data_manager = data_manager
        self.trader = trader
        self.trading_pairs = trading_pairs
        self.telegram_reporter = telegram_reporter

        # Get Telegram credentials
        self.telegram_token = get_telegram_token()
        self.telegram_chat_id = get_telegram_chatId()
        
        # System state
        self.is_running = False
        self.active_positions = {}
        self.pending_orders = {}

        # Performance tracking
        self.performance_metrics = {
            'trades': [],
            'signals': {},
            'positions': {},
            'market_conditions': {}
        }

        # Monitoring
        self.monitor = SystemMonitor(log_dir='../logs/trading_system')

        # Intervals
        self.strategy_update_interval = strategy_update_interval
        self.telegram_update_interval = telegram_update_interval

        # Initialize strategies
        self.strategies = {pair: EMACrossStrategy() for pair in trading_pairs}

        # Set up callbacks
        self._setup_callbacks()

    async def _monitor_system_resources(self):
        """Monitor system resources periodically."""
        while self.is_running:
            try:
                process = psutil.Process()
                memory_info = process.memory_info()

                # Log if memory usage exceeds threshold (e.g., 1.5GB)
                if memory_info.rss > 1.5 * 1024 * 1024 * 1024:  # 1.5GB in bytes
                    logging.warning(f"High memory usage detected: {memory_info.rss / 1024 / 1024:.2f} MB")
                    # Force garbage collection
                    gc.collect()

                await asyncio.sleep(300)  # Check every 5 minutes

            except Exception as e:
                logging.error(f"Error monitoring system resources: {e}")
                await asyncio.sleep(60)
    
    def _setup_callbacks(self):
        """Set up callbacks for real-time updates."""
        self.account.add_order_callback(self._handle_order_update)
        self.account.add_trade_callback(self._handle_trade_update)
        self.account.add_balance_callback(self._handle_balance_update)

    async def start(self):
        """Start the trading system."""
        try:
            self.is_running = True

            # Start system monitor
            asyncio.create_task(self.monitor.start_monitoring())

            # Start resource monitoring
            asyncio.create_task(self._monitor_system_resources())

            # Initialize components
            await self._initialize_system()

            # Start main trading loop
            asyncio.create_task(self._run_trading_loop())

            # Start periodic Telegram updates
            if self.telegram_reporter:
                asyncio.create_task(self._periodic_telegram_updates())

            self.monitor.record_heartbeat('trading_system')
            logging.info("Trading system started successfully.")

        except Exception as e:
            logging.error(f"Error starting trading system: {e}")
            self.is_running = False
            raise

    async def stop(self):
        """Stop the trading system."""
        try:
            self.is_running = False

            # Stop all components
            await self.data_manager.stop_websocket()
            await self.account.stop()
            self.monitor.stop_monitoring()

            logging.info("Trading system stopped successfully.")

        except Exception as e:
            logging.error(f"Error stopping trading system: {e}")
            raise

    async def _initialize_system(self):
        """Initialize all system components."""
        try:
            # Start data manager websocket
            await self.data_manager.start_websocket()

            # Initialize strategies
            for pair in self.trading_pairs:
                data = self.data_manager.get_historical_data(pair)
                if data is not None:
                    self.strategies[pair].update(data)

            # Start account monitoring
            await self.account.start()

            self.monitor.record_heartbeat('system_initialization')
            logging.info("System initialization complete.")

        except Exception as e:
            logging.error(f"Error initializing system: {e}")
            raise

    async def _run_trading_loop_old(self):
        """Main trading loop."""
        while self.is_running:
            try:
                # Update strategies once per hour
                await self._update_strategies()

                # Process each trading pair
                for pair in self.trading_pairs:
                    await self._process_pair(pair)

                # Wait for the next strategy update
                await asyncio.sleep(self.strategy_update_interval)

            except Exception as e:
                logging.error(f"Error in trading loop: {e}")
                await asyncio.sleep(5)  # Wait before retrying

    async def _run_trading_loop(self):
        """Main trading loop with memory management."""
        cleanup_counter = 0
        while self.is_running:
            try:
                # Existing trading logic
                await self._update_strategies()
                for pair in self.trading_pairs:
                    await self._process_pair(pair)

                # Memory management (every 60 iterations)
                cleanup_counter += 1
                if cleanup_counter >= 60:
                    # Clear unnecessary data
                    self.performance_metrics['trades'] = self.performance_metrics['trades'][-1000:]  # Keep last 1000 trades
                    self.performance_metrics['signals'] = {k: v for k, v in self.performance_metrics['signals'].items()
                                                         if (datetime.now() - v['timestamp']).days < 1}

                    # Force garbage collection
                    gc.collect()
                    cleanup_counter = 0

                await asyncio.sleep(self.strategy_update_interval)

            except Exception as e:
                logging.error(f"Error in trading loop: {e}")
                await asyncio.sleep(5)

    async def _update_strategies(self):
        """Update strategies for all trading pairs."""
        for pair in self.trading_pairs:
            try:
                data = self.data_manager.get_historical_data(pair)
                if data is not None:
                    self.strategies[pair].update(data)
                    self.monitor.record_heartbeat(f'strategy_{pair}')
            except Exception as e:
                logging.error(f"Error updating strategy for {pair}: {e}")

    async def _process_pair(self, pair: str):
        """Process a single trading pair."""
        try:
            # Get market data
            market_data = self.data_manager.get_market_snapshot(pair)
            if not market_data:
                return

            # Get strategy signals
            signals = self.strategies[pair].get_all_signals()

            # Check for trade opportunities
            await self._check_trade_opportunities(pair, signals, market_data)

            # Manage existing positions
            await self._manage_positions(pair, market_data)

            # Update performance metrics
            self._update_metrics(pair, signals, market_data)

            self.monitor.record_heartbeat(f'pair_{pair}')

        except Exception as e:
            logging.error(f"Error processing {pair}: {e}")

    async def _check_trade_opportunities(self, pair: str, signals: Dict, market_data: Dict):
        """Check for and execute trade opportunities."""
        try:
            position = self.active_positions.get(pair)

            if position is None:
                # Check for entry signals
                if signals['entries_long'].iloc[-1]:
                    await self._enter_position(pair, 'long', market_data)
                elif signals['entries_short'].iloc[-1]:
                    await self._enter_position(pair, 'short', market_data)
            else:
                # Check for exit signals
                if (position['side'] == 'long' and signals['exits_long'].iloc[-1]) or \
                   (position['side'] == 'short' and signals['exits_short'].iloc[-1]):
                    await self._exit_position(pair, market_data)

        except Exception as e:
            logging.error(f"Error checking trade opportunities for {pair}: {e}")

    async def _enter_position(self, pair: str, side: str, market_data: Dict):
        """Enter a position with a limit order and trailing stop-loss."""
        try:
            # Calculate position size
            position_size = self._calculate_position_size(pair, market_data)

            # Calculate optimal entry price
            entry_price = self._calculate_optimal_entry(side, market_data)

            # Place limit order
            order = await self.trader.place_limit_order(
                symbol=pair,
                side='BUY' if side == 'long' else 'SELL',
                quantity=position_size,
                price=entry_price
            )

            if order:
                self.active_positions[pair] = {
                    'side': side,
                    'entry_price': entry_price,
                    'position_size': position_size,
                    'order_id': order['orderId']
                }

                # Place trailing stop-loss order
                await self.trader.place_trailing_stop_order(
                    symbol=pair,
                    side='SELL' if side == 'long' else 'BUY',
                    quantity=position_size,
                    callback_rate=2.0  # 2% trailing stop
                )

                logging.info(f"Entered {side} position for {pair} at {entry_price}")

        except Exception as e:
            logging.error(f"Error entering position for {pair}: {e}")

    async def _exit_position(self, pair: str, market_data: Dict):
        """Exit a position."""
        try:
            position = self.active_positions.get(pair)
            if not position:
                return

            # Place market order to exit
            await self.trader.place_market_order(
                symbol=pair,
                side='SELL' if position['side'] == 'long' else 'BUY',
                quantity=position['position_size']
            )

            # Remove from active positions
            del self.active_positions[pair]

            logging.info(f"Exited position for {pair}")

        except Exception as e:
            logging.error(f"Error exiting position for {pair}: {e}")

    async def _periodic_telegram_updates(self):
        """Send periodic updates to Telegram."""
        while self.is_running:
            try:
                if self.telegram_reporter:
                    # Send updates on trades, signals, positions, and market conditions
                    #await self.telegram_reporter.report_trades(self.performance_metrics['trades'])
                    #await self.telegram_reporter.report_signals(self.performance_metrics['signals'])
                    #await self.telegram_reporter.report_positions(self.active_positions)
                    #await self.telegram_reporter.report_market_conditions(self.performance_metrics['market_conditions'])
                        # When handling events in the trading system
                    await self.reporter.report_status()

                # Wait for the next update
                await asyncio.sleep(self.telegram_update_interval)

            except Exception as e:
                logging.error(f"Error in periodic Telegram updates: {e}")
                await asyncio.sleep(5)  # Wait before retrying



    def _calculate_position_size(self, pair: str, market_data: Dict) -> float:
        """Calculate position size based on risk management."""
        try:
            # Get account equity
            equity = self.account._calculate_total_balance_usdt()

            # Risk per trade (1% of equity)
            risk_per_trade = 0.01
            position_size = (equity * risk_per_trade) / market_data['price']

            return position_size

        except Exception as e:
            logging.error(f"Error calculating position size for {pair}: {e}")
            return 0.0

    def _calculate_optimal_entry(self, side: str, market_data: Dict) -> float:
        """Calculate optimal entry price based on market conditions."""
        try:
            current_price = market_data['price']
            spread = market_data.get('bid_ask_spread', 0)

            if side == 'long':
                return current_price - (spread * 0.25)  # Place just below current price
            else:
                return current_price + (spread * 0.25)  # Place just above current price

        except Exception as e:
            logging.error(f"Error calculating optimal entry price: {e}")
            return 0.0

    def _update_metrics(self, pair: str, signals: Dict, market_data: Dict):
        """Update performance metrics."""
        try:
            self.performance_metrics['signals'][pair] = {
                'timestamp': datetime.now(),
                'data': signals
            }

            self.performance_metrics['market_conditions'][pair] = {
                'timestamp': datetime.now(),
                'data': market_data
            }

            if pair in self.active_positions:
                self.performance_metrics['positions'][pair] = {
                    'timestamp': datetime.now(),
                    'data': self.active_positions[pair]
                }

        except Exception as e:
            logging.error(f"Error updating metrics for {pair}: {e}")

    async def _handle_order_update(self, order_update: Dict):
        """Handle order status updates."""
        try:
            order_id = order_update.get('orderId')
            if not order_id:
                return

            # Update pending orders
            self.pending_orders[order_id] = order_update

            # Log order status
            logging.info(f"Order update received: {order_update}")

        except Exception as e:
            logging.error(f"Error handling order update: {e}")

    async def _handle_trade_update(self, trade_update: Dict):
        """Handle trade execution updates."""
        try:
            # Update performance metrics
            self.performance_metrics['trades'].append({
                'timestamp': datetime.now(),
                'trade_data': trade_update
            })

            # Log trade execution
            logging.info(f"Trade executed: {trade_update}")

        except Exception as e:
            logging.error(f"Error handling trade update: {e}")

    async def _handle_balance_update(self, asset: str, free: float, locked: float):
        """Handle balance updates."""
        try:
            # Log balance update
            logging.info(f"Balance updated for {asset}: free={free}, locked={locked}")

        except Exception as e:
            logging.error(f"Error handling balance update: {e}")

    async def _manage_positions(self, pair: str, market_data: Dict):
        """Manage existing positions."""
        try:
            position = self.active_positions.get(pair)
            if not position:
                return

            # Check stop loss and take profit
            current_price = market_data['price']
            entry_price = position['entry_price']
            side = position['side']
            
            # Calculate profit/loss
            if side == 'long':
                pnl_pct = (current_price - entry_price) / entry_price * 100
            else:
                pnl_pct = (entry_price - current_price) / entry_price * 100
                
            # Check stop loss
            stop_loss = position.get('stop_loss', entry_price * 0.98 if side == 'long' else entry_price * 1.02)
            take_profit = position.get('take_profit', entry_price * 1.02 if side == 'long' else entry_price * 0.98)
            
            if (side == 'long' and current_price <= stop_loss) or \
               (side == 'short' and current_price >= stop_loss):
                await self._exit_position(pair, 'Stop Loss')
                
            elif (side == 'long' and current_price >= take_profit) or \
                 (side == 'short' and current_price <= take_profit):
                await self._exit_position(pair, 'Take Profit')
                
            # Update position metrics
            position['current_price'] = current_price
            position['pnl'] = pnl_pct
            position['last_update'] = datetime.now(pytz.UTC)
            
            if self.monitor:
                self.monitor.record_heartbeat('position_management')
                
        except Exception as e:
            self.logger.error(f"Error managing position for {pair}: {e}")

    async def _exit_position(self, pair: str, reason: str):
        """Exit a position."""
        try:
            position = self.active_positions.get(pair)
            if not position:
                return

            # Place market order to exit
            await self.trader.place_market_order(
                symbol=pair,
                side='SELL' if position['side'] == 'long' else 'BUY',
                quantity=position['position_size']
            )

            # Log the exit
            self.logger.info(f"Exited position for {pair} due to {reason}")
            
            # Remove from active positions
            del self.active_positions[pair]
            
            if self.monitor:
                self.monitor.record_heartbeat('position_exit')
                
        except Exception as e:
            self.logger.error(f"Error exiting position for {pair}: {e}")

    async def get_system_status(self) -> Dict[str, Any]:
        """Get current system status."""
        try:
            websocket_status = await self.data_manager.check_websocket_status()
            
            # Get memory usage
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            
            return {
                'websocket_connected': websocket_status,
                'active_positions': len(self.active_positions),
                'active_tasks': len([task for task in asyncio.all_tasks() 
                                  if not task.done()]),
                'memory_usage': memory_info.rss / 1024 / 1024,  # Convert to MB
                'last_update': datetime.now(pytz.UTC)
            }
        except Exception as e:
            self.logger.error(f"Error getting system status: {e}")
            return {
                'websocket_connected': False,
                'active_positions': 0,
                'active_tasks': 0,
                'memory_usage': 0,
                'last_update': datetime.now(pytz.UTC),
                'error': str(e)
            }

async def main():
    try:
        # Initialize components
        data_manager = CryptoDataManager(
            symbols=['BTC', 'ETH'],
            base_currency='USDT',
            data_path='../data/market_data.h5',
            historical_days=30
        )

        api_key, api_secret = get_api_key()
        account_manager = BinanceSpotAccount(
            api_key=api_key,
            api_secret=api_secret,
            use_testnet=True,
            tracked_currencies=['BTC', 'ETH', 'USDT']
        )

        trader = BinanceTrader(
            account=account_manager,
            data_manager=data_manager,
            config={
                'max_position_size': 0.1,
                'max_single_order_size': 0.05,
                'min_order_size_usdt': 10.0
            }
        )

        # Verify Telegram credentials
        telegram_token = get_telegram_token()
        telegram_chat_id = get_telegram_chatId()
        if not telegram_token or not telegram_chat_id:
            raise ValueError("Telegram credentials not properly configured")

        # Initialize trading system
        trading_system = IntegratedTradingSystem(
            account_manager=account_manager,
            data_manager=data_manager,
            trader=trader,
            trading_pairs=['BTCUSDT', 'ETHUSDT']
        )

        # Start the system
        await trading_system.start()

        # Initialize and start reporter
        reporter = TradingSystemReporter(trading_system)
        await reporter.start_reporting()

        # Initial position and market update
        await reporter.update_positions_and_markets()

        # Let the system run
        while True:
            await asyncio.sleep(60)  # Sleep for 1 minute
            
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        raise

# Run the system
if __name__ == "__main__":
    asyncio.run(main())

