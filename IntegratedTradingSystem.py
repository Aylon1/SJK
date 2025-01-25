import asyncio
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from tjk_utils.TradingSystemReporter import TradingSystemReporter
from tjk_utils.strategy import EMACrossStrategy
from tjk_utils.SystemMonitor import SystemMonitor
import vectorbtpro as vbt

class IntegratedTradingSystem:
    """
    Integrates strategy, data management, account management, and trade execution.
    Takes advantage of enhanced features in updated components.
    """
    
    def __init__(self,
                 account_manager: 'BinanceSpotAccount',
                 data_manager: 'CryptoDataManager',
                 trader: 'BinanceTrader',
                 trading_pairs: List[str],
                 update_interval: int = 60):  # Default 60 seconds
        """
        Initialize the integrated trading system.
        
        Args:
            account_manager: BinanceSpotAccount instance
            data_manager: CryptoDataManager instance
            trader: BinanceTrader instance
            trading_pairs: List of trading pairs to monitor
            update_interval: Strategy update interval in seconds
        """
        # Initialize system monitor
        self.monitor = None
                    
        self.reporter = TradingSystemReporter(self)
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Core components
        self.account = account_manager
        self.data_manager = data_manager
        self.trader = trader
        self.trading_pairs = trading_pairs
        self.update_interval = update_interval
        
        # Strategy instances for each pair
        self.strategies = {}
        
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
        
        # Set up callbacks
        self._setup_callbacks()

    def _record_heartbeat(self, component: str):
            """Safely record a heartbeat."""
            if hasattr(self, 'monitor') and self.monitor is not None:
                self.monitor.record_heartbeat(component)
    
    def _setup_callbacks(self):
        """Set up callbacks for real-time updates."""
        self.account.add_order_callback(self._handle_order_update)
        self.account.add_trade_callback(self._handle_trade_update)
        self.account.add_balance_callback(self._handle_balance_update)

    async def start_old(self):
        """Start the trading system."""
        try:
            self.is_running = True
            
            # Initialize all components
            await self._initialize_system()
            
            # Start main loop
            await self._run_trading_loop()

            # start telegram reporter
            await self.reporter.start_reporting()
            
        except Exception as e:
            self.logger.error(f"Error starting trading system: {e}")
            raise

    async def start(self):
        """Start the trading system with detailed error handling and timeouts."""
        try:
            self.is_running = True
            
            # Step 1: Initialize components with timeout
            print("Initializing trading system components...")
            try:
                await asyncio.wait_for(self._initialize_system(), timeout=20.0)
                print("System initialization complete")
            except asyncio.TimeoutError:
                print("System initialization timed out!")
                self.is_running = False
                return
    
            # Step 2: Start main trading loop in background
            print("Starting trading loop...")
            self.trading_loop_task = asyncio.create_task(self._run_trading_loop())
            
            # Step 3: Set up monitoring tasks
            print("Setting up monitoring tasks...")
            self.monitor_tasks = []
            
            # Add position monitoring
            self.monitor_tasks.append(
                asyncio.create_task(self._monitor_positions())
            )
            
            # Add system health monitoring
            self.monitor_tasks.append(
                asyncio.create_task(self._monitor_system_health())
            )
    
            print("Trading system started successfully")
            return True
    
        except Exception as e:
            self.logger.error(f"Error starting trading system: {e}")
            self.is_running = False
            return False
    
    async def _monitor_positions(self):
        """Monitor active positions."""
        while self.is_running:
            try:
                for pair in self.trading_pairs:
                    if pair in self.active_positions:
                        # Check position status
                        position = self.active_positions[pair]
                        await self._check_position_status(pair, position)
                await asyncio.sleep(1)  # Check every second
            except Exception as e:
                self.logger.error(f"Error in position monitoring: {e}")
                await asyncio.sleep(5)  # Wait longer on error
    
    async def _monitor_system_health_old(self):
        """Monitor overall system health."""
        while self.is_running:
            try:
                # Check websocket connections
                ws_status = self.data_manager.get_websocket_status()
                if not ws_status['connected']:
                    self.logger.warning("Websocket disconnected, attempting reconnection...")
                    await self.data_manager.restart_websocket()
                
                # Check account connection
                if not await self.account._check_connection():
                    self.logger.warning("Account connection lost, attempting reconnection...")
                    await self.account.stop()
                    await self.account.start()
                
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in system health monitoring: {e}")
                await asyncio.sleep(5)

    async def _monitor_system_health(self):
        """Monitor overall system health."""
        while self.is_running:
            try:
                # Check websocket connections
                ws_status = self.data_manager.get_websocket_status()
                if not ws_status['connected']:
                    self.logger.warning("Websocket disconnected, attempting reconnection...")
                    await self.data_manager.restart_websocket()
                
                # Check account connection status
                account_status = await self.account.check_connection_status()
                if not account_status['api_connected']:
                    self.logger.warning("Account connection lost, attempting reconnection...")
                    await self.account.stop()
                    await self.account.start()
                
                # Record system heartbeat
                self._record_heartbeat('system_health')
                
                # Wait before next check
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in system health monitoring: {e}")
                await asyncio.sleep(5)  # Shorter wait on error
    
    async def _run_trading_loop(self):
        """Main trading loop with enhanced error handling."""
        while self.is_running:
            try:
                for pair in self.trading_pairs:
                    self._record_heartbeat('trading_loop')
                    try:
                        await asyncio.wait_for(
                            self._process_pair(pair),
                            timeout=5.0  # 5 second timeout per pair
                        )
                    except asyncio.TimeoutError:
                        self.logger.warning(f"Processing {pair} timed out")
                    except Exception as e:
                        self.logger.error(f"Error processing {pair}: {e}")
                        continue
                
                await asyncio.sleep(1)  # Process every second
                
            except Exception as e:
                self.logger.error(f"Error in trading loop: {e}")
                await asyncio.sleep(5)  # Wait longer on error
    
    async def _check_position_status(self, pair: str, position: Dict):
        """Check and update position status."""
        try:
            market_data = self.data_manager.get_market_snapshot(pair)
            if not market_data:
                return
            
            current_price = market_data['price']
            
            # Update position metrics
            position['current_price'] = current_price
            position['pnl'] = (current_price - position['entry_price']) * position['position_size']
            position['pnl_pct'] = (current_price - position['entry_price']) / position['entry_price'] * 100
            
            # Check stop conditions
            await self._check_stop_conditions(pair, position)
            
        except Exception as e:
            self.logger.error(f"Error checking position status for {pair}: {e}")
    
    async def _check_stop_conditions(self, pair: str, position: Dict):
        """Check and handle stop conditions."""
        try:
            # Get strategy signals
            signals = self.strategies[pair].get_all_signals()
            
            # Check exit signals
            if ((position['side'] == 'long' and signals['exits_long'].iloc[-1]) or
                (position['side'] == 'short' and signals['exits_short'].iloc[-1])):
                await self.trader.place_market_order(
                    symbol=pair,
                    side='SELL' if position['side'] == 'long' else 'BUY',
                    quantity=position['position_size']
                )
                del self.active_positions[pair]
                
        except Exception as e:
            self.logger.error(f"Error checking stop conditions for {pair}: {e}")

    async def stop(self):
        """Stop the trading system."""
        try:
            self.is_running = False
            
            # Close all positions if configured to do so
            # await self._close_all_positions()
            
            # Stop data streams
            await self.data_manager.stop_websocket()
            
            # Stop account monitoring
            await self.account.stop()
            
            self.logger.info("Trading system stopped successfully")
            
        except Exception as e:
            self.logger.error(f"Error stopping trading system: {e}")
            raise

    async def _initialize_system_old(self):
        """Initialize all system components."""
        try:
            # Start data manager websocket connections
            await self.data_manager.start_websocket()
            
            # Wait for initial data
            await asyncio.sleep(5)  # Allow time for initial data
            
            # Initialize strategies for each pair
            for pair in self.trading_pairs:
                self.strategies[pair] = EMACrossStrategy()
                await self._initialize_strategy(pair)
            
            # Verify data quality
            await self._verify_data_quality()
            
            # Initialize account monitoring
            self.account.start()
            
            self.logger.info("Trading system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing system: {e}")
            raise

    async def _initialize_strategy(self, pair: str) -> None:
        """Initialize strategy for a trading pair."""
        try:
            if pair not in self.strategies:
                self.strategies[pair] = EMACrossStrategy()

            # Get data from data manager
            data = self.data_manager.get_historical_data(pair)
            if data is not None:
                # Convert to vbt.Data if needed
                if not isinstance(data, vbt.Data):
                    data = vbt.Data.from_data(data)
                    
                # Initialize strategy with data
                success = self.strategies[pair].update(data)
                if success:
                    self.logger.info(f"Strategy initialized successfully for {pair}")
                else:
                    self.logger.error(f"Failed to initialize strategy for {pair}")
            else:
                self.logger.error(f"No historical data available for {pair}")

        except Exception as e:
            self.logger.error(f"Error initializing strategy for {pair}: {e}")
            raise

    async def _initialize_system(self):
        """Initialize all system components."""
        try:
            # Start data manager websocket connections
            await self.data_manager.start_websocket()
            
            # Initialize strategies for each pair
            for pair in self.trading_pairs:
                await self._initialize_strategy(pair)
            
            # Verify data quality
            await self._verify_data_quality()
            
            self.logger.info("Trading system initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Error initializing system: {e}")
            raise
    
    async def _verify_data_quality(self):
        """Verify data quality for all trading pairs."""
        for pair in self.trading_pairs:
            metrics = self.data_manager.get_data_quality_metrics(pair)
            if metrics.get('completeness_ratio', 0) < 0.99:
                self.logger.warning(f"Data quality issues detected for {pair}")

    async def _run_trading_loop(self):
        """Main trading loop with enhanced market analysis."""
        while self.is_running:
            try:
                # Record heartbeat for main loop
                self._record_heartbeat('main_loop')
                
                for pair in self.trading_pairs:
                    # Process market conditions and trading logic
                    await self._process_pair(pair)
                    
                    # Update market analysis
                    await self._update_market_analysis(pair)
                    
                    # Check risk metrics
                    await self._check_risk_metrics(pair)
                
                # Wait for next update interval
                await asyncio.sleep(self.update_interval)
                
            except Exception as e:
                self.logger.error(f"Error in trading loop: {e}")
                await asyncio.sleep(5)  # Wait before retrying

    async def _process_pair(self, pair: str):
        """Process a single trading pair with enhanced market analysis."""
        try:
            self._record_heartbeat('process_pair')
            
            # Get comprehensive market data
            market_data = self.data_manager.get_market_snapshot(pair)
            if not market_data:
                return
            
            # Get order book analysis
            order_book = self.data_manager.get_order_book_metrics(pair)
            
            # Get liquidity analysis
            liquidity = self.data_manager.get_liquidity_analysis(pair)
            
            # Update strategy with enhanced data
            strategy = self.strategies[pair]
            signals = strategy.get_all_signals()
            
            # Check for trade opportunities with enhanced analysis
            await self._check_trade_opportunities(pair, signals, market_data, order_book, liquidity)

            # telegram signals
            await self.reporter.report_strategy_signal(pair, signals)
            
            # Update active positions with trailing stops
            await self._manage_positions(pair, market_data)
            
            # Update performance metrics
            self._update_metrics(pair, signals, market_data)

            # telegram
            await self.reporter.report_market_snapshot(pair, market_data)
        except Exception as e:
            self.logger.error(f"Error processing {pair}: {e}")

    async def _check_trade_opportunities(self, pair: str, signals: Dict, 
                                       market_data: Dict, order_book: Dict, 
                                       liquidity: Dict):
        """Check for and execute trade opportunities with enhanced analysis."""
        try:
            # Get current position
            position = self.active_positions.get(pair)
            
            # Calculate optimal position size based on risk and liquidity
            position_size = self._calculate_position_size(pair, order_book, liquidity)
            
            if position is None:
                # Check for entry opportunities
                if signals['entries_long'].iloc[-1]:
                    await self._enter_position(pair, 'long', position_size, market_data, order_book)
                elif signals['entries_short'].iloc[-1]:
                    await self._enter_position(pair, 'short', position_size, market_data, order_book)
            else:
                # Check for exit opportunities
                if (position['side'] == 'long' and signals['exits_long'].iloc[-1]) or \
                   (position['side'] == 'short' and signals['exits_short'].iloc[-1]):
                    await self._exit_position(pair, market_data, order_book)
                    
        except Exception as e:
            self.logger.error(f"Error checking trade opportunities for {pair}: {e}")

    async def _enter_position(self, pair: str, side: str, position_size: float, 
                            market_data: Dict, order_book: Dict):
        """Enter a position with enhanced execution logic."""
        try:
            # Check risk limits
            if not self._check_risk_limits(pair, position_size, side):
                return
            
            # Calculate optimal entry price based on order book
            entry_price = self._calculate_optimal_entry(side, order_book)
            
            # Calculate dynamic stop loss based on volatility
            stop_loss = self._calculate_dynamic_stop(side, market_data)
            
            # Place order with slippage control
            order = await self.trader.place_limit_order_with_trailing_stop(
                symbol=pair,
                side='BUY' if side == 'long' else 'SELL',
                quantity=position_size,
                price=entry_price,
                callback_rate=stop_loss['callback_rate']
            )
            
            if order:
                self.active_positions[pair] = {
                    'side': side,
                    'entry_price': entry_price,
                    'position_size': position_size,
                    'entry_time': datetime.now(),
                    'order_id': order['orderId'],
                    'stop_loss': stop_loss
                }
                
                self.logger.info(f"Entered {side} position for {pair}")

                # telegram order
                await self.reporter.report_trade_execution(order)
        
        except Exception as e:
            self.logger.error(f"Error entering position for {pair}: {e}")

    def _calculate_position_size(self, pair: str, order_book: Dict, liquidity: Dict) -> float:
        """Calculate optimal position size based on risk and liquidity."""
        try:
            # Get account equity
            equity = self.account._calculate_total_balance_usdt()
            
            # Get risk per trade (from trader config)
            risk_per_trade = self.trader.config['position_sizing']['risk_per_trade']
            
            # Calculate base position size from equity
            base_size = equity * risk_per_trade
            
            # Adjust based on liquidity
            liquidity_score = liquidity.get('imbalance', 0)
            if abs(liquidity_score) > 0.5:  # Significant imbalance
                base_size *= 0.5  # Reduce size in low liquidity
                
            # Ensure size doesn't exceed order book depth
            max_size = min(
                order_book.get('bid_depth', float('inf')),
                order_book.get('ask_depth', float('inf'))
            ) * 0.1  # Use max 10% of available liquidity
            
            return min(base_size, max_size)
            
        except Exception as e:
            self.logger.error(f"Error calculating position size: {e}")
            return 0.0

    def _calculate_optimal_entry(self, side: str, order_book: Dict) -> float:
        """Calculate optimal entry price based on order book analysis."""
        try:
            spread = order_book.get('bid_ask_spread', 0)
            mid_price = order_book.get('weighted_mid_price', 0)
            
            if side == 'long':
                return mid_price + (spread * 0.25)  # Place just above mid
            else:
                return mid_price - (spread * 0.25)  # Place just below mid
                
        except Exception as e:
            self.logger.error(f"Error calculating optimal entry: {e}")
            return 0.0

    def _calculate_dynamic_stop(self, side: str, market_data: Dict) -> Dict:
        """Calculate dynamic stop loss based on market conditions."""
        try:
            volatility = market_data.get('volatility', 0)
            atr = market_data.get('atr', 0)
            
            # Base callback rate on volatility
            base_rate = max(0.01, min(0.03, volatility / 100))
            
            # Adjust based on ATR
            atr_adjustment = atr / market_data.get('price', 1) * 0.5
            
            callback_rate = base_rate + atr_adjustment
            
            return {
                'callback_rate': callback_rate,
                'volatility_level': volatility,
                'atr_level': atr
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating dynamic stop: {e}")
            return {'callback_rate': 0.01}  # Default fallback

    async def _update_market_analysis(self, pair: str):
        """Update market analysis metrics."""
        try:
            market_conditions = self.trader.analyze_market_conditions(pair)
            
            self.performance_metrics['market_conditions'][pair] = {
                'timestamp': datetime.now(),
                'analysis': market_conditions
            }
            
        except Exception as e:
            self.logger.error(f"Error updating market analysis for {pair}: {e}")

    async def _check_risk_metrics(self, pair: str):
        """Check and update risk metrics."""
        try:
            portfolio_metrics = self.trader.calculate_portfolio_metrics()
            
            # Check risk thresholds
            if portfolio_metrics['risk_metrics']['risk_level'] in ['high', 'very_high']:
                self.logger.warning(f"High risk level detected for {pair}")
                # Implement risk reduction measures if needed


            # telegram risk
            await self.reporter.report_risk_update(portfolio_metrics)
            
        except Exception as e:
            self.logger.error(f"Error checking risk metrics for {pair}: {e}")

    def get_system_status_old(self) -> Dict:
        """Get comprehensive system status."""
        try:
            status = {
                'is_running': self.is_running,
                'active_positions': self.active_positions,
                'pending_orders': self.pending_orders,
                'performance_metrics': self.performance_metrics,
                'data_manager_status': self.data_manager.get_websocket_status(),
                'market_snapshots': {}
            }
            
            # Add market snapshots
            for pair in self.trading_pairs:
                try:
                    status['market_snapshots'][pair] = self.data_manager.get_market_snapshot(pair)
                except Exception as e:
                    self.logger.error(f"Error getting market snapshot for {pair}: {e}")
                    status['market_snapshots'][pair] = None
            
            # Add account status if available
            try:
                status['account_status'] = self.account.get_formatted_balance()
            except Exception as e:
                self.logger.error(f"Error getting account status: {e}")
                status['account_status'] = None
            
            # Add connection stats if available
            try:
                status['connection_stats'] = self.data_manager.get_connection_statistics()
            except Exception as e:
                self.logger.error(f"Error getting connection stats: {e}")
                status['connection_stats'] = None
                
            return status
            
        except Exception as e:
            self.logger.error(f"Error getting system status: {e}")
            return {
                'is_running': self.is_running,
                'error': str(e)
            }

    def get_system_status(self) -> Dict:
        """Get comprehensive system status."""
        try:
            status = {
                'is_running': self.is_running,
                'active_positions': self.active_positions,
                'pending_orders': self.pending_orders,
                'performance_metrics': self.performance_metrics,
                'data_manager_status': self.data_manager.get_websocket_status()
            }
            
            # Get market snapshots
            status['market_snapshots'] = {}
            for pair in self.trading_pairs:
                try:
                    if not pair.endswith('USDT'):
                        continue
                    status['market_snapshots'][pair] = self.data_manager.get_market_snapshot(pair)
                except Exception as e:
                    self.logger.error(f"Error getting market snapshot for {pair}: {e}")
                    status['market_snapshots'][pair] = None
            
            # Get account status
            try:
                status['account_status'] = self.account.get_formatted_balance()
            except Exception as e:
                self.logger.error(f"Error getting account status: {e}")
                status['account_status'] = None
            
            # Get connection stats
            try:
                status['connection_stats'] = self.data_manager.get_connection_statistics()
            except Exception as e:
                self.logger.error(f"Error getting connection stats: {e}")
                status['connection_stats'] = None
                
            # Get portfolio metrics
            try:
                status['portfolio_metrics'] = self.trader.calculate_portfolio_metrics()
            except Exception as e:
                self.logger.error(f"Error getting portfolio metrics: {e}")
                status['portfolio_metrics'] = None
                
            return status
            
        except Exception as e:
            self.logger.error(f"Error getting system status: {e}")
            return {
                'is_running': self.is_running,
                'error': str(e)
            }
    
    def _handle_order_update(self, order_update: Dict):
        """Handle order status updates."""
        try:
            order_id = order_update.get('orderId')
            if not order_id:
                return
            
            # Update pending orders
            self.pending_orders[order_id] = order_update
            
            # Log order status
            self.logger.info(f"Order update received: {order_update}")
            
        except Exception as e:
            self.logger.error(f"Error handling order update: {e}")

    def _handle_trade_update(self, trade_update: Dict):
        """Handle trade execution updates."""
        try:
            # Update performance metrics
            self.performance_metrics['trades'].append({
                'timestamp': datetime.now(),
                'trade_data': trade_update
            })
            
            # Log trade execution
            self.logger.info(f"Trade executed: {trade_update}")
            
        except Exception as e:
            self.logger.error(f"Error handling trade update: {e}")

    def _handle_balance_update(self, asset: str, free: float, locked: float):
        """Handle balance updates."""
        try:
            # Update portfolio metrics
            portfolio_metrics = self.trader.calculate_portfolio_metrics()
            
            # Check for significant changes
            if portfolio_metrics['risk_metrics']['risk_level'] in ['high', 'very_high']:
                self.logger.warning(f"High risk level detected after balance update for {asset}")
                asyncio.create_task(self._risk_adjustment_check())
            
            # Log balance update
            self.logger.info(f"Balance updated for {asset}: free={free}, locked={locked}")
            
        except Exception as e:
            self.logger.error(f"Error handling balance update: {e}")

    async def _risk_adjustment_check(self):
        """Check and adjust positions based on risk levels."""
        try:
            portfolio_metrics = self.trader.calculate_portfolio_metrics()
            risk_level = portfolio_metrics['risk_metrics']['risk_level']
            
            if risk_level in ['high', 'very_high']:
                # Calculate position reduction needed
                exposure = portfolio_metrics['exposure_metrics']['gross_exposure']
                target_exposure = exposure * 0.8  # Reduce by 20%
                
                # Reduce positions starting with most risky
                await self._reduce_positions(target_exposure)
                
        except Exception as e:
            self.logger.error(f"Error in risk adjustment check: {e}")

    async def _reduce_positions(self, target_exposure: float):
        """Reduce positions to meet target exposure."""
        try:
            # Sort positions by risk level
            positions = sorted(
                self.active_positions.items(),
                key=lambda x: self._calculate_position_risk(x[1]),
                reverse=True
            )
            
            for pair, position in positions:
                if await self._should_reduce_position(pair, position):
                    await self._reduce_single_position(pair, position)
                    
                # Check if target exposure reached
                current_metrics = self.trader.calculate_portfolio_metrics()
                if current_metrics['exposure_metrics']['gross_exposure'] <= target_exposure:
                    break
                    
        except Exception as e:
            self.logger.error(f"Error reducing positions: {e}")

    def _calculate_position_risk(self, position: Dict) -> float:
        """Calculate risk score for a single position."""
        try:
            # Get market data
            pair = position.get('symbol', '')
            market_data = self.data_manager.get_market_snapshot(pair)
            
            if not market_data:
                return float('inf')  # Consider unknown risk as highest
            
            # Calculate risk factors
            volatility_risk = market_data.get('volatility', 0) / 100
            size_risk = position['position_size'] * market_data.get('price', 0) / self.account._calculate_total_balance_usdt()
            duration_risk = min(1.0, (datetime.now() - position['entry_time']).days / 7)  # Cap at 7 days
            
            # Get market conditions
            market_conditions = self.trader.analyze_market_conditions(pair)
            liquidity_risk = 1 - market_conditions.get('market_microstructure', {}).get('liquidity_score', 50) / 100
            
            # Calculate weighted risk score
            risk_score = (
                volatility_risk * 0.3 +
                size_risk * 0.3 +
                duration_risk * 0.2 +
                liquidity_risk * 0.2
            )
            
            return risk_score
            
        except Exception as e:
            self.logger.error(f"Error calculating position risk: {e}")
            return float('inf')

    async def _should_reduce_position(self, pair: str, position: Dict) -> bool:
        """Determine if a position should be reduced."""
        try:
            # Get market conditions
            market_data = self.data_manager.get_market_snapshot(pair)
            if not market_data:
                return True  # Reduce if can't get market data
            
            # Check various factors
            risk_score = self._calculate_position_risk(position)
            
            # Get current market conditions
            market_conditions = self.trader.analyze_market_conditions(pair)
            liquidity = market_conditions.get('market_microstructure', {}).get('liquidity_score', 0)
            
            # Decision factors
            high_risk = risk_score > 0.7
            poor_liquidity = liquidity < 30
            large_loss = self._calculate_unrealized_pnl(position) < -0.05  # 5% loss
            
            return high_risk or poor_liquidity or large_loss
            
        except Exception as e:
            self.logger.error(f"Error checking position reduction: {e}")
            return False

    async def _reduce_single_position(self, pair: str, position: Dict):
        """Reduce a single position size."""
        try:
            # Calculate reduction amount (reduce by 50%)
            reduction_size = position['position_size'] * 0.5
            
            # Get market conditions for optimal execution
            order_book = self.data_manager.get_order_book_metrics(pair)
            
            # Place reduction order
            await self.trader.place_market_order(
                symbol=pair,
                side='SELL' if position['side'] == 'long' else 'BUY',
                quantity=reduction_size,
                max_slippage=0.001  # 0.1% max slippage
            )
            
            # Update position size
            position['position_size'] -= reduction_size
            
            self.logger.info(f"Reduced position for {pair} by {reduction_size}")
            
        except Exception as e:
            self.logger.error(f"Error reducing position for {pair}: {e}")

    def _calculate_unrealized_pnl(self, position: Dict) -> float:
        """Calculate unrealized P&L for a position."""
        try:
            current_price = self.data_manager.get_latest_price(position.get('symbol', ''))
            if not current_price:
                return 0.0
            
            entry_price = position['entry_price']
            position_size = position['position_size']
            
            if position['side'] == 'long':
                return (current_price - entry_price) / entry_price
            else:
                return (entry_price - current_price) / entry_price
                
        except Exception as e:
            self.logger.error(f"Error calculating unrealized PnL: {e}")
            return 0.0

    def get_performance_summary(self) -> Dict:
        """Get comprehensive performance summary."""
        try:
            return {
                'portfolio_metrics': self.trader.calculate_portfolio_metrics(),
                'trading_performance': {
                    'total_trades': len(self.performance_metrics['trades']),
                    'winning_trades': len([t for t in self.performance_metrics['trades'] 
                                         if t.get('pnl', 0) > 0]),
                    'total_pnl': sum(t.get('pnl', 0) for t in self.performance_metrics['trades'])
                },
                'risk_metrics': self.trader.calculate_portfolio_metrics(),
                'market_analysis': {
                    pair: self.trader.analyze_market_conditions(pair)
                    for pair in self.trading_pairs
                },
                'system_health': {
                    'data_quality': {
                        pair: self.data_manager.get_data_quality_metrics(pair)
                        for pair in self.trading_pairs
                    },
                    'connection_status': self.data_manager.get_websocket_status(),
                    'last_update': datetime.now()
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting performance summary: {e}")
            return {}

    async def _manage_positions(self, pair: str, market_data: Dict):
        """Manage existing positions for a trading pair."""
        try:
            # Get current position
            position = self.active_positions.get(pair)
            if not position:
                return

            # Get latest signals
            strategy = self.strategies.get(pair)
            if not strategy:
                return

            signals = strategy.get_all_signals()
            
            # Check exit signals
            if position['side'] == 'long' and signals['exits_long'].iloc[-1]:
                await self._exit_position(pair, market_data)
            elif position['side'] == 'short' and signals['exits_short'].iloc[-1]:
                await self._exit_position(pair, market_data)

        except Exception as e:
            self.logger.error(f"Error managing positions for {pair}: {e}")

    async def _exit_position(self, pair: str, market_data: Dict):
        """Exit a position."""
        try:
            position = self.active_positions.get(pair)
            if not position:
                return

            # Place exit order through trader
            await self.trader.place_market_order(
                symbol=pair,
                side='SELL' if position['side'] == 'long' else 'BUY',
                quantity=position['position_size']
            )

            # Remove from active positions
            del self.active_positions[pair]

        except Exception as e:
            self.logger.error(f"Error exiting position for {pair}: {e}")

    def _update_metrics(self, pair: str, signals: Dict, market_data: Dict):
        """Update performance metrics for a trading pair."""
        try:
            # Store signal information
            self.performance_metrics['signals'][pair] = {
                'timestamp': datetime.now(),
                'data': signals
            }
            
            # Store market data
            self.performance_metrics['market_conditions'][pair] = {
                'timestamp': datetime.now(),
                'data': market_data
            }
            
            # Store position information if exists
            position = self.active_positions.get(pair)
            if position:
                self.performance_metrics['positions'][pair] = {
                    'timestamp': datetime.now(),
                    'data': position
                }
                
        except Exception as e:
            self.logger.error(f"Error updating metrics for {pair}: {e}")