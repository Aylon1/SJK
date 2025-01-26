import logging
from typing import Dict, List, Optional, Union, Any, Tuple
from datetime import datetime, timedelta
import asyncio, threading
from threading import Lock
import pandas as pd
import numpy as np
from decimal import Decimal
import time, uuid
import pytz

class BinanceTrader:
    def __init__(self, account: 'BinanceSpotAccount', data_manager: 'CryptoDataManager', config: Optional[Dict] = None):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.account, self.data_manager = account, data_manager
        self.lock = threading.Lock()
        self.config = {
            'max_position_size': 0.5, 'max_single_order_size': 0.5,
            'min_order_size_usdt': 10.0, 'default_stop_loss_pct': 0.02,
            'default_take_profit_pct': 0.04, 'max_slippage_pct': 0.001,
            'volatility_thresholds': {'low': 0.5, 'medium': 2.0},
            'position_sizing': {
                'method': 'risk_based', 'risk_per_trade': 0.01,
                'max_risk_multiplier': 2.0
            },
            'retry': {'max_attempts': 3, 'delay_seconds': 1}
        }
        if config: self.config.update(config)
        self.order_lock, self.position_lock, self.trade_lock = Lock(), Lock(), Lock()
        self.active_orders, self.active_positions = {}, {}
        self.trade_history, self.trailing_stops = [], {}
        self.performance_metrics = {'trades': [], 'daily_pnl': {}, 'execution_quality': {}}
        self._setup_callbacks()

    def _setup_callbacks(self):
        self.account.add_order_callback(self._handle_order_update)
        self.account.add_trade_callback(self._handle_trade_update)

# Add these methods to the BinanceTrader class

    async def _handle_order_update(self, order_update: Dict):
        """Handle order status updates."""
        try:
            order_id = order_update.get('orderId')
            if not order_id:
                return
    
            with self.order_lock:
                # Update order tracking
                self.active_orders[order_id] = order_update
    
                # Update performance metrics
                if order_update.get('status') == 'FILLED':
                    await self._process_filled_order(order_id, order_update)
                elif order_update.get('status') == 'CANCELED':
                    await self._process_cancelled_order(order_id, order_update)
    
            self.logger.info(f"Order update processed: {order_id}")
    
        except Exception as e:
            self.logger.error(f"Error handling order update: {str(e)}")

    async def _process_filled_order(self, order_id: str, order_update: Dict):
        """Process a filled order."""
        try:
            order = self.active_orders[order_id]
            symbol = order['symbol']
    
            # Update position
            with self.position_lock:
                if symbol not in self.active_positions:
                    self.active_positions[symbol] = {
                        'quantity': 0,
                        'entry_price': 0,
                        'total_cost': 0
                    }
    
                position = self.active_positions[symbol]
                qty_delta = float(order_update['executedQty'])
                if order_update['side'] == 'SELL':
                    qty_delta = -qty_delta
    
                new_qty = position['quantity'] + qty_delta
                if new_qty == 0:
                    del self.active_positions[symbol]
                else:
                    position['quantity'] = new_qty
                    position['entry_price'] = float(order_update['price'])
                    position['last_update'] = datetime.now()
    
            # Record trade
            self._record_trade(order_update)
            del self.active_orders[order_id]
    
        except Exception as e:
            self.logger.error(f"Error processing filled order: {str(e)}")

    async def _process_cancelled_order(self, order_id: str, order_update: Dict):
        """Process a cancelled order."""
        try:
            if order_id in self.active_orders:
                order = self.active_orders[order_id]
                symbol = order['symbol']
                
                # Update tracking
                with self.order_lock:
                    del self.active_orders[order_id]
                
                # Log cancellation
                self.logger.info(f"Order {order_id} for {symbol} cancelled")
                
                # Cleanup any associated stops
                if order_id in self.trailing_stops:
                    del self.trailing_stops[order_id]
                    
        except Exception as e:
            self.logger.error(f"Error processing cancelled order: {str(e)}")

    def _handle_trade_update(self, trade_update: Dict):
        """Handle trade execution updates."""
        try:
            trade_id = trade_update.get('id')
            if not trade_id:
                return
    
            with self.lock:
                self._update_execution_metrics(trade_update)
                self._update_position_tracking(trade_update)
    
            self.logger.info(f"Trade update processed: {trade_id}")
    
        except Exception as e:
            self.logger.error(f"Error handling trade update: {str(e)}")

    def _update_execution_metrics(self, trade_update: Dict):
        """Update execution quality metrics."""
        try:
            trade_id = trade_update.get('id')
            with self.lock:
                execution_data = {
                    'symbol': trade_update.get('symbol'),
                    'price': float(trade_update.get('price', 0)),
                    'quantity': float(trade_update.get('qty', 0)),
                    'time': trade_update.get('time', datetime.now().timestamp()),
                    'market_impact': self._estimate_market_impact(
                        trade_update.get('symbol', ''),
                        float(trade_update.get('qty', 0))
                    )
                }
                self.performance_metrics['execution_quality'][trade_id] = execution_data
    
        except Exception as e:
            self.logger.error(f"Error updating execution metrics: {str(e)}")

    def _update_position_tracking(self, trade_update: Dict):
        """Update internal position tracking."""
        try:
            symbol = trade_update.get('symbol')
            quantity = float(trade_update.get('qty', 0))
            side = trade_update.get('side', '').upper()
    
            with self.position_lock:
                position = self.active_positions.setdefault(
                    symbol, {'quantity': 0, 'average_price': 0}
                )
                old_quantity = position['quantity']
    
                if side == 'BUY':
                    position['quantity'] += quantity
                    if quantity > 0:
                        # Update average price for buys
                        total_cost = (old_quantity * position['average_price'] +
                                    quantity * float(trade_update.get('price', 0)))
                        position['average_price'] = total_cost / position['quantity']
                else:
                    position['quantity'] -= quantity
    
                # Remove position if quantity is zero
                if position['quantity'] == 0:
                    del self.active_positions[symbol]
    
        except Exception as e:
            self.logger.error(f"Error updating position tracking: {str(e)}")

    def _validate_order_params(self, symbol: str, side: str, quantity: float, price: Optional[float] = None) -> bool:
        """Validate order parameters."""
        try:
            if symbol not in self.data_manager.trading_pairs:
                self.logger.error(f"Invalid symbol: {symbol}")
                return False
    
            if side not in ['BUY', 'SELL']:
                self.logger.error(f"Invalid side: {side}")
                return False
    
            if quantity <= 0:
                self.logger.error(f"Invalid quantity: {quantity}")
                return False
    
            if price is not None:
                market_data = self.data_manager.get_market_snapshot(symbol)
                current_price = market_data['price']
    
                # Check if price is within reasonable range (±20% of current price)
                if not (current_price * 0.8 <= price <= current_price * 1.2):
                    self.logger.warning(f"Price {price} significantly different from market price {current_price}")
    
            return True
    
        except Exception as e:
            self.logger.error(f"Error validating order parameters: {str(e)}")
            return False
            
    async def place_limit_order(self, symbol: str, side: str, quantity: float, price: float) -> Dict:
        """Place a limit order."""
        try:
            if not self._validate_order_params(symbol, side, quantity):
                raise ValueError("Invalid order parameters")
    
            formatted_quantity = self.account.format_number_to_binance_precision(
                symbol, quantity, is_price=False
            )
            formatted_price = self.account.format_number_to_binance_precision(
                symbol, price, is_price=True
            )
    
            order_params = {
                'symbol': symbol,
                'side': side,
                'type': 'LIMIT',
                'quantity': formatted_quantity,
                'price': formatted_price,
                'timeInForce': 'GTC'  # Good 'til canceled
            }
    
            # Check if create_order is synchronous or asynchronous
            if asyncio.iscoroutinefunction(self.account.client.create_order):
                order = await self.account.client.create_order(**order_params)
            else:
                order = self.account.client.create_order(**order_params)
    
            return order
    
        except Exception as e:
            self.logger.error(f"Error placing limit order: {str(e)}")
            raise
        
    async def place_market_order(self, symbol: str, side: str, quantity: float, max_slippage: Optional[float] = None) -> Dict:
        """Place a market order with optional slippage control."""
        try:
            if not self._validate_order_params(symbol, side, quantity):
                raise ValueError("Invalid order parameters")

            formatted_quantity = self.account.format_number_to_binance_precision(
                symbol, quantity, is_price=False
            )

            # Get current market price for slippage check
            if max_slippage:
                market_data = self.data_manager.get_market_snapshot(symbol)
                current_price = market_data['price']
                
                # Calculate price bounds based on slippage
                max_price = current_price * (1 + max_slippage)
                min_price = current_price * (1 - max_slippage)
                
                order_params = {
                    'symbol': symbol,
                    'side': side,
                    'type': 'MARKET',
                    'quantity': formatted_quantity,
                    'price': self.account.format_number_to_binance_precision(
                        symbol, max_price if side == 'BUY' else min_price, is_price=True
                    )
                }
            else:
                order_params = {
                    'symbol': symbol,
                    'side': side,
                    'type': 'MARKET',
                    'quantity': formatted_quantity
                }
            
            # Check if create_order is synchronous or asynchronous
            if asyncio.iscoroutinefunction(self.account.client.create_order):
                # If create_order is async, await it directly
                order = await self.account.client.create_order(**order_params)
            else:
                # If create_order is synchronous, call it directly
                order = self.account.client.create_order(**order_params)
            
            #order = await self.account.client.create_order(**order_params)
            return order

        except Exception as e:
            self.logger.error(f"Error placing market order: {str(e)}")
            raise

    async def cancel_order(self, symbol: str, order_id: str = None, cancel_all: bool = False) -> Union[Dict, List[Dict]]:
        """
        Cancel one or all orders for a symbol.
        
        Args:
            symbol: Trading pair symbol
            order_id: Specific order ID to cancel (optional)
            cancel_all: Whether to cancel all open orders for the symbol
        
        Returns:
            Dict or List[Dict]: Cancellation response(s) from exchange
        """
        try:
            if cancel_all:
                # Cancel all orders for symbol
                with self.order_lock:
                    response = await self.account.client.cancel_all_orders(symbol=symbol)
                    # Clean up local order tracking
                    self.active_orders = {
                        oid: order for oid, order in self.active_orders.items()
                        if order['symbol'] != symbol
                    }
                    self.logger.info(f"Cancelled all orders for {symbol}")
                    return response
            else:
                if not order_id:
                    raise ValueError("Must provide order_id when not using cancel_all")
                    
                # Cancel specific order
                with self.order_lock:
                    response = await self.account.client.cancel_order(
                        symbol=symbol,
                        orderId=order_id
                    )
                    # Remove from local tracking
                    if order_id in self.active_orders:
                        del self.active_orders[order_id]
                    self.logger.info(f"Cancelled order {order_id} for {symbol}")
                    return response
    
        except Exception as e:
            self.logger.error(f"Error cancelling order(s) for {symbol}: {str(e)}")
            raise
    
    async def cancel_all_for_symbols(self, symbols: List[str]) -> Dict[str, Any]:
        """
        Cancel all orders for multiple symbols.
        
        Args:
            symbols: List of trading pair symbols
        
        Returns:
            Dict containing results for each symbol
        """
        try:
            results = {}
            for symbol in symbols:
                try:
                    result = await self.cancel_order(symbol, cancel_all=True)
                    results[symbol] = {
                        'status': 'success',
                        'response': result
                    }
                except Exception as e:
                    results[symbol] = {
                        'status': 'error',
                        'error': str(e)
                    }
            return results
        
        except Exception as e:
            self.logger.error(f"Error in cancel_all_for_symbols: {str(e)}")
            raise
    
    async def get_open_orders_for_symbol(self, symbol: str) -> List[Dict]:
        """
        Get all open orders for a specific symbol.
        
        Args:
            symbol: Trading pair symbol
        
        Returns:
            List[Dict]: List of open orders
        """
        try:
            with self.order_lock:
                return [
                    order for order in self.active_orders.values()
                    if order['symbol'] == symbol and order['status'] == 'NEW'
                ]
        except Exception as e:
            self.logger.error(f"Error getting open orders for {symbol}: {str(e)}")
            raise
    
    def _calculate_ideal_limit_price(self, symbol: str, side: str, quantity: float, timeframe_minutes: int = 5) -> float:
        try:
            market_data = self.data_manager.get_market_snapshot(symbol)
            if not market_data:
                raise ValueError("No market data available")
            
            current_price = market_data['price']
            if not current_price:
                raise ValueError("Invalid current price")
                
            # Get order book metrics with fallback values
            order_book_metrics = self.data_manager.get_order_book_metrics(symbol) or {}
            bid_ask_spread = order_book_metrics.get('bid_ask_spread', current_price * 0.001)  # Default to 0.1% spread
            imbalance_ratio = order_book_metrics.get('imbalance_ratio', 0)
            
            # Get some basic price adjustments
            expected_impact = self._estimate_market_impact(symbol, quantity)
            volatility_adjustment = self._calculate_atr(symbol) or (current_price * 0.001)  # Default to 0.1% volatility
            
            if side == 'BUY':
                base_adjustment = -(volatility_adjustment + bid_ask_spread / 2)
                if imbalance_ratio > 0.2:
                    base_adjustment *= 0.8
                ideal_price = current_price + base_adjustment - (expected_impact * 1.2)
            else:
                base_adjustment = volatility_adjustment + bid_ask_spread / 2
                if imbalance_ratio < -0.2:
                    base_adjustment *= 0.8
                ideal_price = current_price + base_adjustment + (expected_impact * 1.2)
            
            # Ensure price is within reasonable bounds
            max_deviation = current_price * 0.02  # 2% max deviation
            ideal_price = max(current_price - max_deviation, min(current_price + max_deviation, ideal_price))
            
            return float(self.account.format_number_to_binance_precision(symbol, ideal_price, is_price=True))
            
        except Exception as e:
            self.logger.error(f"Error calculating ideal limit price: {str(e)}")
            # Fallback to current market price with small adjustment
            if market_data and market_data.get('price'):
                adjustment = 0.999 if side == 'SELL' else 1.001  # 0.1% adjustment
                return float(market_data['price'] * adjustment)
            raise

    async def place_limit_order_with_trailing_stop(self, symbol: str, side: str, quantity: float, callback_rate: float, allow_partial: bool = True) -> Dict:
        """Place a limit order with a trailing stop."""
        try:
            # Calculate the ideal limit price
            price = self._calculate_ideal_limit_price(symbol, side, quantity)
            
            # Validate order parameters
            if not self._validate_order_params(symbol, side, quantity, price):
                raise ValueError("Invalid order parameters")
            
            # Check risk limits
            if not self._check_risk_limits(symbol, side, quantity, price):
                raise ValueError("Order exceeds risk limits")
    
            # Format quantity and price according to Binance precision
            formatted_quantity = self.account.format_number_to_binance_precision(
                symbol, quantity, is_price=False
            )
            formatted_price = self.account.format_number_to_binance_precision(
                symbol, price, is_price=True
            )
    
            # Prepare order parameters
            order_params = {
                'symbol': symbol,
                'side': side,
                'type': 'LIMIT',
                'timeInForce': 'GTC',  # Good 'til canceled
                'quantity': formatted_quantity,
                'price': formatted_price
            }
    
            # Place the limit order
            with self.order_lock:
                # Check if create_order is synchronous or asynchronous
                if asyncio.iscoroutinefunction(self.account.client.create_order):
                    order = await self.account.client.create_order(**order_params)
                else:
                    order = self.account.client.create_order(**order_params)
    
                # Store the order and trailing stop details
                order_id = order['orderId']
                self.active_orders[order_id] = {
                    'order': order,
                    'trailing_stop': {
                        'callback_rate': callback_rate,
                        'activation_price': price,
                        'stop_price': self._calculate_initial_stop_price(side, price, callback_rate),
                        'allow_partial': allow_partial
                    }
                }
    
                # Start monitoring the trailing stop
                asyncio.create_task(self._monitor_trailing_stop(order_id, symbol, side))
    
                return order
    
        except Exception as e:
            self.logger.error(f"Error placing limit order with trailing stop: {str(e)}")
            raise
                
    async def place_scaled_order(self, symbol: str, side: str, total_quantity: float, price_range: Tuple[float, float], num_orders: int = 5, distribution: str = 'linear') -> List[Dict]:
        """Place multiple limit orders at scaled prices."""
        try:
            start_price, end_price = price_range
            prices = np.linspace(start_price, end_price, num_orders) if distribution == 'linear' else np.geomspace(start_price, end_price, num_orders)
            quantities = [total_quantity / num_orders] * num_orders
    
            # Place each limit order
            orders = []
            for price, qty in zip(prices, quantities):
                order = await self.place_limit_order(
                    symbol=symbol,
                    side=side,
                    quantity=qty,
                    price=price
                )
                orders.append(order)
    
            return orders
    
        except Exception as e:
            self.logger.error(f"Error placing scaled orders: {str(e)}")
            raise


    async def place_twap_order(self, symbol: str, side: str, total_quantity: float, duration_minutes: int, num_orders: int) -> Dict:
        """Place a TWAP order."""
        try:
            quantity_per_order = total_quantity / num_orders
            interval_minutes = duration_minutes / num_orders
    
            # Store TWAP details
            twap_details = {
                'symbol': symbol,
                'side': side,
                'total_quantity': total_quantity,
                'executed_quantity': 0,
                'orders': [],
                'start_time': datetime.now(),
                'end_time': datetime.now() + timedelta(minutes=duration_minutes)
            }
    
            # Start the TWAP execution task
            asyncio.create_task(self._execute_twap(
                symbol=symbol,
                side=side,
                quantity_per_order=quantity_per_order,
                interval_minutes=interval_minutes,
                num_orders=num_orders,
                twap_details=twap_details
            ))
    
            return twap_details
    
        except Exception as e:
            self.logger.error(f"Error setting up TWAP order: {str(e)}")
            raise

    async def _execute_twap(self, symbol: str, side: str, quantity_per_order: float, interval_minutes: float, num_orders: int, twap_details: Dict):
        """Execute a TWAP order by placing orders at regular intervals."""
        try:
            for i in range(num_orders):
                # Wait for the specified interval
                await asyncio.sleep(interval_minutes * 60)  # Convert minutes to seconds
    
                # Place the order
                try:
                    order = await self.place_limit_order(
                        symbol=symbol,
                        side=side,
                        quantity=quantity_per_order,
                        price=self.data_manager.get_market_snapshot(symbol)['price']  # Use current market price
                    )
                    twap_details['orders'].append(order)
                    twap_details['executed_quantity'] += quantity_per_order
                except Exception as e:
                    self.logger.error(f"Error placing TWAP order {i + 1}/{num_orders}: {str(e)}")
    
        except Exception as e:
            self.logger.error(f"Error executing TWAP order: {str(e)}")

    async def place_oco_order(self, symbol: str, side: str, quantity: float, price: float, stop_price: float, stop_limit_price: float) -> Dict:
        """Place an OCO (One-Cancels-the-Other) order."""
        try:
            # Get the current market price
            market_price = self.data_manager.get_market_snapshot(symbol)['price']
            self.logger.info(f"Current market price for {symbol}: {market_price}")
    
            # Retrieve symbol filters
            symbol_info = self.account.client.get_symbol_info(symbol)
            if not symbol_info:
                raise ValueError(f"Symbol {symbol} not found")
    
            price_filter = next(filter(lambda x: x['filterType'] == 'PRICE_FILTER', symbol_info['filters']))
            min_price = float(price_filter['minPrice'])
            max_price = float(price_filter['maxPrice'])
            tick_size = float(price_filter['tickSize'])
    
            self.logger.info(f"Symbol filters for {symbol}: minPrice={min_price}, maxPrice={max_price}, tickSize={tick_size}")
    
            # Validate prices against market price
            if side == 'BUY':
                if price >= market_price:
                    self.logger.warning(f"Price {price} is not below market price {market_price}. Adjusting...")
                    price = market_price * 0.99  # Set price 1% below market price
                if stop_price <= market_price:
                    self.logger.warning(f"Stop price {stop_price} is not above market price {market_price}. Adjusting...")
                    stop_price = market_price * 1.01  # Set stop price 1% above market price
            elif side == 'SELL':
                if price <= market_price:
                    self.logger.warning(f"Price {price} is not above market price {market_price}. Adjusting...")
                    price = market_price * 1.01  # Set price 1% above market price
                if stop_price >= market_price:
                    self.logger.warning(f"Stop price {stop_price} is not below market price {market_price}. Adjusting...")
                    stop_price = market_price * 0.99  # Set stop price 1% below market price
    
            # Ensure stopLimitPrice is valid
            if stop_limit_price <= stop_price:
                self.logger.warning(f"Stop limit price {stop_limit_price} is not above stop price {stop_price}. Adjusting...")
                stop_limit_price = stop_price * 1.01  # Set stop limit price 1% above stop price
    
            # Adjust prices to comply with symbol filters
            def adjust_price(value: float, tick_size: float) -> float:
                """Adjust price to comply with tick size."""
                return round(value // tick_size * tick_size, 8)
    
            price = adjust_price(price, tick_size)
            stop_price = adjust_price(stop_price, tick_size)
            stop_limit_price = adjust_price(stop_limit_price, tick_size)
    
            # Ensure prices are within allowed range
            price = max(min(price, max_price), min_price)
            stop_price = max(min(stop_price, max_price), min_price)
            stop_limit_price = max(min(stop_limit_price, max_price), min_price)
    
            self.logger.info(f"Adjusted prices: price={price}, stopPrice={stop_price}, stopLimitPrice={stop_limit_price}")
    
            # Validate order parameters
            if not self._validate_order_params(symbol, side, quantity, price):
                raise ValueError("Invalid order parameters")
    
            # Format quantities and prices
            formatted_qty = self.account.format_number_to_binance_precision(symbol, quantity, is_price=False)
            formatted_price = self.account.format_number_to_binance_precision(symbol, price, is_price=True)
            formatted_stop = self.account.format_number_to_binance_precision(symbol, stop_price, is_price=True)
            formatted_stop_limit = self.account.format_number_to_binance_precision(symbol, stop_limit_price, is_price=True)
    
            # Prepare OCO order parameters
            order_params = {
                'symbol': symbol,
                'side': side,
                'quantity': formatted_qty,
                'price': formatted_price,
                'stopPrice': formatted_stop,
                'stopLimitPrice': formatted_stop_limit,
                'stopLimitTimeInForce': 'GTC'
            }
    
            # Place the OCO order
            with self.order_lock:
                # Check if create_oco_order is synchronous or asynchronous
                if asyncio.iscoroutinefunction(self.account.client.create_oco_order):
                    order = await self.account.client.create_oco_order(**order_params)
                else:
                    order = self.account.client.create_oco_order(**order_params)
    
                # Store the OCO order details
                self.active_orders[order['orderListId']] = {
                    'order': order,
                    'type': 'OCO',
                    'status': 'NEW'
                }
    
                return order
    
        except Exception as e:
            self.logger.error(f"Error placing OCO order: {str(e)}")
            raise

    async def adjust_position_size(self, symbol: str, target_size: float, max_slippage: float = None) -> Dict[str, Any]:
        try:
            with self.position_lock:
                current_position = self.active_positions.get(symbol, {'quantity': 0})
                current_size = float(current_position.get('quantity', 0))
                if current_size == target_size: return {'status': 'no_action_needed'}
                
                size_difference = target_size - current_size
                side = 'BUY' if size_difference > 0 else 'SELL'
                quantity = abs(size_difference)
                market_data = self.data_manager.get_market_snapshot(symbol)
                
                if not self._check_risk_limits(symbol, side, quantity, market_data['price']):
                    raise ValueError("Position adjustment exceeds risk limits")
                return await self.place_twap_order(symbol, side, quantity, 30, 5) if abs(size_difference) > self.config['max_single_order_size'] else await self.place_market_order(symbol, side, quantity, max_slippage or self.config['max_slippage_pct'])
        except Exception as e:
            self.logger.error(f"Error adjusting position size: {str(e)}")
            raise

    async def rebalance_portfolio(self, target_weights: Dict[str, float], tolerance: float = 0.02) -> Dict[str, Any]:
        try:
            total_value = await self.account._calculate_total_balance_usdt()
            current_positions = self.account.get_position_exposure()
            rebalance_orders = []
            
            for symbol, target_weight in target_weights.items():
                current_weight = current_positions.get(symbol, {'exposure_pct': 0}).get('exposure_pct', 0) / 100
                if abs(current_weight - target_weight) > tolerance:
                    target_value = total_value * target_weight
                    current_value = total_value * current_weight
                    value_difference = target_value - current_value
                    market_data = self.data_manager.get_market_snapshot(symbol)
                    quantity = abs(value_difference / market_data['price'])
                    side = 'BUY' if value_difference > 0 else 'SELL'
                    order = await self.adjust_position_size(symbol, quantity)
                    rebalance_orders.append(order)
            
            return {'status': 'completed', 'orders': rebalance_orders, 'timestamp': datetime.now()}
        except Exception as e:
            self.logger.error(f"Error rebalancing portfolio: {str(e)}")
            raise

    def _record_trade(self, order_update: Dict):
        try:
            trade = {
                'symbol': order_update['symbol'],
                'side': order_update['side'],
                'quantity': float(order_update['executedQty']),
                'price': float(order_update['price']),
                'commission': float(order_update.get('commission', 0)),
                'commission_asset': order_update.get('commissionAsset', 'USDT'),
                'time': pd.Timestamp(order_update['time']),
                'order_id': str(order_update['orderId']),
                'trade_id': str(order_update.get('tradeId', uuid.uuid4())),
                'pnl': 0,
                'pnl_pct': 0
            }
            
            if trade['side'] == 'SELL':
                entry_trades = [t for t in self.trade_history
                              if t['symbol'] == trade['symbol'] and t['side'] == 'BUY']
                if entry_trades:
                    entry_price = np.mean([t['price'] for t in entry_trades])
                    trade['pnl'] = (trade['price'] - entry_price) * trade['quantity']
                    trade['pnl_pct'] = (trade['price'] - entry_price) / entry_price
            
            self.trade_history.append(trade)
        except Exception as e:
            self.logger.error(f"Error recording trade: {str(e)}")

    def generate_performance_report(self, timeframe: str = '1d') -> Dict:
        try:
            start_time = datetime.now() - timedelta(days={'1d': 1, '1w': 7, '1m': 30, 'all': 365}[timeframe])
            trades = [t for t in self.trade_history if t['time'] >= start_time]
            if not trades: return {"status": "no_trades", "timeframe": timeframe}
            
            trades_df = pd.DataFrame(trades)
            trades_df.set_index('time', inplace=True)
            
            daily_returns = trades_df.groupby(trades_df.index.date)['pnl'].sum()
            cumulative_returns = daily_returns.cumsum()
            
            return {
                'summary': {
                    'timeframe': timeframe,
                    'total_trades': len(trades),
                    'winning_trades': len(trades_df[trades_df['pnl'] > 0]),
                    'losing_trades': len(trades_df[trades_df['pnl'] < 0]),
                    'total_pnl': trades_df['pnl'].sum(),
                    'avg_trade_pnl': trades_df['pnl'].mean(),
                    'largest_win': trades_df['pnl'].max(),
                    'largest_loss': trades_df['pnl'].min(),
                },
                'risk_metrics': {
                    'sharpe_ratio': self._calculate_sharpe_ratio(trades),
                    'sortino_ratio': self._calculate_sortino_ratio(trades),
                    'max_drawdown': self._calculate_max_drawdown_from_equity(cumulative_returns.values),
                    'volatility': daily_returns.std(),
                    'var_95': float(np.percentile(daily_returns, 5)),
                },
                'daily_pnl': daily_returns.to_dict(),
                'cumulative_returns': cumulative_returns.to_dict(),
                'generated_at': datetime.now()
            }

        except Exception as e:
                    self.logger.error(f"Error generating performance report: {str(e)}")
                    return {"status": "error", "message": str(e)}

    
    def analyze_market_conditions(self, symbol: str) -> Dict:
        """
        Analyze current market conditions with safe division handling.
        """
        try:
            market_data = self.data_manager.get_market_snapshot(symbol)
            if not market_data:
                return {"status": "error", "message": "Unable to fetch market data"}
    
            # Get order book metrics
            order_book_metrics = self.data_manager.get_order_book_metrics(symbol)
            if not order_book_metrics:
                order_book_metrics = {
                    'bid_ask_spread': 0,
                    'bid_ask_spread_pct': 0,
                    'bid_depth': 0,
                    'ask_depth': 0,
                    'bid_ask_ratio': 0,
                    'weighted_mid_price': 0,
                    'price_impact_buy': 0,
                    'price_impact_sell': 0
                }
    
            # Get historical data
            historical_data = self.data_manager.get_historical_data(
                symbol,
                start_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            )
    
            if historical_data is None or historical_data.empty:
                return {"status": "error", "message": "No historical data available"}
    
            # Calculate micro structure metrics safely
            bid_depth = order_book_metrics.get('bid_depth', 0)
            ask_depth = order_book_metrics.get('ask_depth', 0)
            depth_imbalance = (bid_depth / ask_depth) if ask_depth > 0 else 0
    
            # Calculate microstructure metrics using actual field names
            micro_structure = {
                'bid_ask_spread': order_book_metrics.get('bid_ask_spread', 0),
                'bid_ask_imbalance': order_book_metrics.get('bid_ask_ratio', 0) - 1,  # Convert ratio to imbalance
                'depth_imbalance': order_book_metrics.get('bid_depth', 0) / order_book_metrics.get('ask_depth', 1) if order_book_metrics.get('ask_depth', 0) > 0 else 0,
                'volume_momentum': self._calculate_volume_momentum(
                    symbol, historical_data, market_data.get('price', 0)
                ),
                'price_impact': {
                    'buy': order_book_metrics.get('price_impact_buy', 0),
                    'sell': order_book_metrics.get('price_impact_sell', 0)
                },
                'weighted_mid_price': order_book_metrics.get('weighted_mid_price', market_data.get('price', 0))
            }
    
            # Calculate regime indicators
            regime_indicators = {
                'trend_strength': self._calculate_trend_strength(historical_data),
                'volatility_regime': self._classify_volatility_regime(market_data.get('volatility', 0)),
                'liquidity_score': self._calculate_liquidity_score(symbol),
                'market_efficiency_ratio': self._calculate_market_efficiency_ratio(historical_data)
            }
    
            return {
                'symbol': symbol,
                'timestamp': datetime.now(),
                'price_metrics': {
                    'current_price': market_data.get('price', 0),
                    'daily_change': market_data.get('price_change_24h', 0),
                    'daily_high': market_data.get('high_24h', 0),
                    'daily_low': market_data.get('low_24h', 0),
                    'volatility': market_data.get('volatility', 0),
                    'atr': self._calculate_atr(symbol)
                },
                'volume_analysis': {
                    'daily_volume': market_data.get('volume_24h', 0),
                    'volume_profile': self._calculate_volume_profile(historical_data),
                    'volume_trend': self._analyze_volume_trend(historical_data)
                },
                'market_microstructure': micro_structure,
                'market_regime': regime_indicators
            }
    
        except Exception as e:
            self.logger.error(f"Error analyzing market conditions: {str(e)}")
            return {"status": "error", "message": str(e)}

    def _analyze_volume_trend(self, historical_data: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze volume trend patterns from historical data.
        """
        try:
            if historical_data is None or historical_data.empty:
                return {
                    'trend': 'neutral',
                    'volume_ratio_20': 0.0,
                    'volume_ratio_50': 0.0,
                    'current_volume': 0.0,
                    'average_volume_20': 0.0,
                    'average_volume_50': 0.0,
                    'volume_volatility': 0.0,
                    'is_consistent': False
                }
    
            volume = historical_data['Volume']
            current_volume = volume.iloc[-1]
            volume_sma_20 = volume.rolling(window=20).mean()
            volume_sma_50 = volume.rolling(window=50).mean()
    
            avg_volume_20 = volume_sma_20.iloc[-1] if not pd.isna(volume_sma_20.iloc[-1]) else current_volume
            avg_volume_50 = volume_sma_50.iloc[-1] if not pd.isna(volume_sma_50.iloc[-1]) else current_volume
    
            volume_ratio_20 = current_volume / avg_volume_20 if avg_volume_20 > 0 else 1.0
            volume_ratio_50 = current_volume / avg_volume_50 if avg_volume_50 > 0 else 1.0
    
            # Calculate volume consistency
            volume_std = volume.rolling(window=20).std().fillna(0)
            volume_cv = np.where(volume_sma_20 > 0, volume_std / volume_sma_20, 0)
            
            # Determine trend
            if volume_ratio_20 > 1.5 and volume_ratio_50 > 1.5:
                trend = 'strong_increase'
            elif volume_ratio_20 > 1.2 and volume_ratio_50 > 1.2:
                trend = 'moderate_increase'
            elif volume_ratio_20 < 0.5 and volume_ratio_50 < 0.5:
                trend = 'strong_decrease'
            elif volume_ratio_20 < 0.8 and volume_ratio_50 < 0.8:
                trend = 'moderate_decrease'
            else:
                trend = 'neutral'
    
            return {
                'trend': trend,
                'volume_ratio_20': float(volume_ratio_20),
                'volume_ratio_50': float(volume_ratio_50),
                'current_volume': float(current_volume),
                'average_volume_20': float(avg_volume_20),
                'average_volume_50': float(avg_volume_50),
                'volume_volatility': float(volume_cv[-1]) if len(volume_cv) > 0 else 0.0,
                'is_consistent': float(volume_cv[-1]) < 0.5 if len(volume_cv) > 0 else False
            }
    
        except Exception as e:
            self.logger.error(f"Error analyzing volume trend: {str(e)}")
            return {
                'trend': 'neutral',
                'volume_ratio_20': 0.0,
                'volume_ratio_50': 0.0,
                'current_volume': 0.0,
                'average_volume_20': 0.0,
                'average_volume_50': 0.0,
                'volume_volatility': 0.0,
                'is_consistent': False
            }

    def _calculate_liquidity_score(self, symbol: str) -> float:
        """
        Calculate liquidity score based on order book metrics.
        Returns a score between 0 and 100, where higher values indicate better liquidity.
    
        Args:
            symbol: Trading pair symbol
    
        Returns:
            float: Liquidity score (0-100)
        """
        try:
            # Get order book metrics
            order_book_metrics = self.data_manager.get_order_book_metrics(symbol)
            if not order_book_metrics:
                return 50.0  # Return neutral score if no metrics available
    
            # Get recent trade data
            market_data = self.data_manager.get_market_snapshot(symbol)
            if not market_data:
                return 50.0
    
            # Calculate depth score (0-100)
            total_depth = (
                order_book_metrics.get('bid_depth', 0) + 
                order_book_metrics.get('ask_depth', 0)
            )
            volume_24h = market_data.get('volume_24h', 0)
            depth_score = min(100, (total_depth / volume_24h * 100)) if volume_24h > 0 else 50.0
    
            # Calculate spread score (0-100)
            # Lower spread = higher score
            spread_pct = order_book_metrics.get('bid_ask_spread_pct', 0)
            spread_score = 100 * (1 - min(1, spread_pct / 0.5))  # 0.5% spread = threshold
    
            # Calculate imbalance penalty (0-100)
            # Lower imbalance = higher score
            imbalance = abs(order_book_metrics.get('imbalance_ratio', 0))
            imbalance_penalty = imbalance * 20  # 20% penalty per unit of imbalance
    
            # Calculate final weighted score
            weights = {
                'depth': 0.5,        # 50% weight to depth
                'spread': 0.3,       # 30% weight to spread
                'imbalance': 0.2     # 20% weight to imbalance
            }
    
            final_score = (
                depth_score * weights['depth'] +
                spread_score * weights['spread'] +
                (100 - imbalance_penalty) * weights['imbalance']
            )
    
            # Ensure score is within bounds
            return max(0, min(100, final_score))
    
        except Exception as e:
            self.logger.error(f"Error calculating liquidity score: {str(e)}")
            return 50.0  # Return neutral score on error
    
    def _check_portfolio_concentration(self, symbol: str, order_value: float) -> bool:
        """
        Check if adding this order would exceed portfolio concentration limits.
        
        Args:
            symbol: Trading pair symbol
            order_value: Value of the order in USDT
            
        Returns:
            bool: True if concentration is acceptable, False otherwise
        """
        try:
            total_portfolio = self.account._calculate_total_balance_usdt()
            if total_portfolio == 0:
                return False
    
            # Get current position value
            current_position = self.active_positions.get(symbol, {'quantity': 0})
            market_data = self.data_manager.get_market_snapshot(symbol)
            current_price = market_data.get('price', 0)
            current_value = float(current_position.get('quantity', 0)) * current_price
    
            # Calculate new total position value
            new_total = current_value + order_value
    
            # Check concentration limit (e.g., no single position > 20% of portfolio)
            concentration_limit = 0.20  # 20%
            if new_total / total_portfolio > concentration_limit:
                self.logger.warning(f"Order would exceed concentration limit of {concentration_limit*100}%")
                return False
    
            # Calculate portfolio Herfindahl-Hirschman Index (HHI)
            position_weights = []
            for pos_symbol, position in self.active_positions.items():
                pos_market_data = self.data_manager.get_market_snapshot(pos_symbol)
                pos_price = pos_market_data.get('price', 0)
                pos_value = float(position.get('quantity', 0)) * pos_price
                position_weights.append(pos_value / total_portfolio)
    
            # Add new position weight
            position_weights.append(new_total / total_portfolio)
    
            # Calculate HHI
            hhi = sum(w * w for w in position_weights)
    
            # Check HHI limit (e.g., HHI should not exceed 0.25)
            hhi_limit = 0.25
            if hhi > hhi_limit:
                self.logger.warning(f"Order would exceed portfolio HHI limit of {hhi_limit}")
                return False
    
            return True
    
        except Exception as e:
            self.logger.error(f"Error checking portfolio concentration: {str(e)}")
            return False
    
    def _get_min_quantity(self, symbol: str) -> float:
        """
        Get minimum allowed quantity for a symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            float: Minimum allowed quantity
        """
        try:
            # Get market data
            market_data = self.data_manager.get_market_snapshot(symbol)
            current_price = market_data.get('price', 0)
            
            if current_price == 0:
                return 0.0
    
            # Calculate minimum quantity based on minimum USDT value
            min_quantity = self.config['min_order_size_usdt'] / current_price
            
            # Format to exchange precision
            return float(self.account.format_number_to_binance_precision(
                symbol, min_quantity, is_price=False
            ))
    
        except Exception as e:
            self.logger.error(f"Error getting minimum quantity: {str(e)}")
            return 0.0
    
    def _calculate_market_efficiency_ratio(self, historical_data: pd.DataFrame) -> float:
        """Calculate market efficiency ratio with safe division."""
        try:
            if historical_data is None or historical_data.empty:
                return 0.0
    
            close_prices = historical_data['Close']
            directional_movement = abs(close_prices.iloc[-1] - close_prices.iloc[0])
            total_movement = abs(close_prices.diff()).sum()
    
            if total_movement == 0:
                return 0.0
    
            efficiency_ratio = directional_movement / total_movement
            return float(efficiency_ratio)
    
        except Exception as e:
            self.logger.error(f"Error calculating market efficiency ratio: {str(e)}")
            return 0.0
    
    def _calculate_atr(self, symbol: str, period: int = 14) -> float:
        try:
            historical_data = self.data_manager.get_historical_data(
                symbol, start_date=(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            )
            if historical_data is None or historical_data.empty: return 0.0
            
            high, low, close = historical_data['High'].values, historical_data['Low'].values, historical_data['Close'].values
            tr1, tr2, tr3 = np.abs(high[1:] - low[1:]), np.abs(high[1:] - close[:-1]), np.abs(low[1:] - close[:-1])
            tr = np.maximum(np.maximum(tr1, tr2), tr3)
            return float(np.mean(tr))
        except Exception as e:
            self.logger.error(f"Error calculating ATR: {str(e)}")
            return 0.0

    def _calculate_volume_profile(self, historical_data: pd.DataFrame) -> Dict:
        try:
            if historical_data is None or historical_data.empty:
                return {'volume_by_price': {}, 'poc_price': 0, 'value_area': {'low': 0, 'high': 0}}
            
            price_data, volume_data = historical_data['Close'], historical_data['Volume']
            price_bins = pd.qcut(price_data, q=10, duplicates='drop')
            volume_profile = volume_data.groupby(price_bins).sum()
            
            poc_level = volume_profile.idxmax()
            total_volume = volume_profile.sum()
            sorted_profile = volume_profile.sort_values(ascending=False)
            cumsum_volume = sorted_profile.cumsum()
            value_area_mask = cumsum_volume <= (total_volume * 0.68)
            value_area_levels = sorted_profile[value_area_mask].index
            
            return {
                'volume_by_price': volume_profile.to_dict(),
                'poc_price': float(poc_level.mid),
                'value_area': {
                    'low': float(value_area_levels.min().left),
                    'high': float(value_area_levels.max().right)
                }
            }
        except Exception as e:
            self.logger.error(f"Error calculating volume profile: {str(e)}")
            return {'volume_by_price': {}, 'poc_price': 0, 'value_area': {'low': 0, 'high': 0}}

    def _calculate_volume_momentum(self, symbol: str, historical_data: pd.DataFrame, current_price: float) -> float:
        """Calculate volume momentum with safe division."""
        try:
            if historical_data is None or historical_data.empty:
                return 0.0
    
            total_volume = historical_data['Volume'].sum()
            if total_volume == 0:
                return 0.0
    
            vwap = (historical_data['Close'] * historical_data['Volume']).sum() / total_volume
            if vwap == 0:
                return 0.0
    
            momentum = (current_price - vwap) / vwap
            return max(min(momentum, 1.0), -1.0)
    
        except Exception as e:
            self.logger.error(f"Error calculating volume momentum: {str(e)}")
            return 0.0

    def _classify_volatility_regime(self, volatility: float) -> str:
        """
        Classify current volatility regime.
    
        Args:
            volatility: Volatility value to classify
    
        Returns:
            str: 'low', 'medium', or 'high' based on thresholds
        """
        try:
            if volatility <= self.config['volatility_thresholds']['low']:
                return 'low'
            elif volatility <= self.config['volatility_thresholds']['medium']:
                return 'medium'
            else:
                return 'high'
        except Exception as e:
            self.logger.error(f"Error classifying volatility regime: {str(e)}")
            return 'medium'  # Default to medium volatility regime
    
    def _calculate_trend_strength(self, historical_data: pd.DataFrame) -> float:
        """
        Calculate trend strength using Directional Movement Index (DMI).
        Returns ADX value as trend strength indicator.
        """
        try:
            if historical_data is None or historical_data.empty or len(historical_data) < 15:
                return 0.0
    
            historical_data = historical_data.copy()
            if not isinstance(historical_data.index, pd.DatetimeIndex):
                historical_data.index = pd.to_datetime(historical_data.index)
    
            period = 14
            eps = 1e-8  # Increased epsilon
            
            # Basic calculations remain the same up to PDI and NDI
            high = historical_data['High'].values[1:]
            low = historical_data['Low'].values[1:]
            prev_high = historical_data['High'].values[:-1]
            prev_low = historical_data['Low'].values[:-1]
            prev_close = historical_data['Close'].values[:-1]
            close = historical_data['Close'].values[1:]
    
            tr1 = high - low
            tr2 = np.abs(high - prev_close)
            tr3 = np.abs(low - prev_close)
            tr = np.maximum(np.maximum(tr1, tr2), tr3)
    
            up_move = high - prev_high
            down_move = prev_low - low
            
            pos_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0)
            neg_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0)
    
            def smooth(data, period):
                return pd.Series(data).rolling(window=period, min_periods=1).mean().fillna(0).values
    
            tr_smooth = smooth(tr, period)
            pos_dm_smooth = smooth(pos_dm, period)
            neg_dm_smooth = smooth(neg_dm, period)
    
            # Use masked arrays to handle division
            tr_smooth_masked = np.ma.masked_where(tr_smooth <= eps, tr_smooth)
            pdi = np.ma.filled(100 * pos_dm_smooth / tr_smooth_masked, 0)
            ndi = np.ma.filled(100 * neg_dm_smooth / tr_smooth_masked, 0)
    
            # Calculate DX using masked array for division
            denominator = pdi + ndi
            denominator_masked = np.ma.masked_where(denominator <= eps, denominator)
            dx = np.ma.filled(100 * np.abs(pdi - ndi) / denominator_masked, 0)
    
            # Calculate ADX using rolling mean
            adx = float(pd.Series(dx).rolling(window=period, min_periods=1).mean().fillna(0).iloc[-1])
    
            # Ensure valid output
            if np.isnan(adx) or np.isinf(adx):
                return 0.0
    
            return min(100.0, max(0.0, adx))
    
        except Exception as e:
            self.logger.error(f"Error calculating trend strength: {str(e)}")
            return 0.0


    def _check_risk_limits(self, symbol: str, side: str, quantity: float, price: float) -> bool:
        try:
            total_portfolio = self.account._calculate_total_balance_usdt()
            order_value = quantity * price
            current_position = self.active_positions.get(symbol, {'quantity': 0})
            new_position_size = float(current_position['quantity']) + (quantity if side == 'BUY' else -quantity)
            new_position_value = new_position_size * price
            
            if new_position_value / total_portfolio > self.config['max_position_size']:
                self.logger.error("Position size would exceed maximum allowed")
                return False
            
            if order_value / total_portfolio > self.config['max_single_order_size']:
                self.logger.error("Order size exceeds maximum allowed")
                return False
            
            if order_value < self.config['min_order_size_usdt']:
                self.logger.error("Order size below minimum allowed")
                return False
            
            return True
        except Exception as e:
            self.logger.error(f"Error checking risk limits: {str(e)}")
            return False

    async def _monitor_trailing_stop(self, order_id: str, symbol: str, side: str):
        try:
            while order_id in self.active_orders:
                order_info = self.active_orders[order_id]
                trailing_stop = order_info['trailing_stop']
                market_data = self.data_manager.get_market_snapshot(symbol)
                current_price = market_data['price']
                
                if side == 'BUY':
                    new_stop_price = current_price * (1 - trailing_stop['callback_rate'])
                    if new_stop_price > trailing_stop['stop_price']:
                        trailing_stop['stop_price'] = new_stop_price
                else:
                    new_stop_price = current_price * (1 + trailing_stop['callback_rate'])
                    if new_stop_price < trailing_stop['stop_price']:
                        trailing_stop['stop_price'] = new_stop_price
                
                if self._is_stop_triggered(side, current_price, trailing_stop['stop_price']):
                    await self._execute_stop_order(order_id, symbol, side)
                    break
                
                await asyncio.sleep(1)
        except Exception as e:
            self.logger.error(f"Error monitoring trailing stop: {str(e)}")

    def _is_stop_triggered(self, side: str, current_price: float, stop_price: float) -> bool:
        return (side == 'BUY' and current_price <= stop_price) or (side == 'SELL' and current_price >= stop_price)

    async def _execute_stop_order(self, order_id: str, symbol: str, side: str):
        try:
            order = self.active_orders[order_id]
            await self.account.cancel_order(symbol=symbol, orderId=order_id)
            market_side = 'SELL' if side == 'BUY' else 'BUY'
            await self.place_market_order(
                symbol=symbol,
                side=market_side,
                quantity=order['order']['origQty']
            )
            del self.active_orders[order_id]
        except Exception as e:
            self.logger.error(f"Error executing stop order: {str(e)}")

    def _calculate_max_drawdown_from_equity(self, equity_curve: np.ndarray) -> float:
        try:
            rolling_max = np.maximum.accumulate(equity_curve)
            drawdowns = (rolling_max - equity_curve) / rolling_max
            return float(np.max(drawdowns)) if len(drawdowns) > 0 else 0.0
        except Exception as e:
            self.logger.error(f"Error calculating max drawdown: {str(e)}")
            return 0.0

    def calculate_portfolio_metrics(self) -> dict:
        """
        Calculate comprehensive portfolio metrics.
        
        Returns:
            dict: Dictionary containing portfolio metrics including:
                - total_value: Total portfolio value in USDT
                - positions: List of current positions
                - pnl_24h: 24-hour profit/loss
                - risk_metrics: Various risk measurements
                - exposure_metrics: Position exposure information
                - performance_metrics: Trading performance statistics
        """
        try:
            # Get account balances
            balances = self.account.get_formatted_balance(min_value=0.0)
            
            # Get current positions
            positions = self.account.get_position_exposure()
            
            # Calculate total portfolio value
            total_value = self.account._calculate_total_balance_usdt()
            
            # Get 24h PnL
            daily_pnl = self.get_daily_pnl()
            
            # Calculate risk metrics
            risk_metrics = self._calculate_risk_metrics()
            
            # Calculate position exposure
            exposure_metrics = self._calculate_exposure_metrics(positions, total_value)
            
            # Get performance metrics
            performance = self.generate_performance_report(timeframe='1d')
            
            return {
                'total_value': total_value,
                'positions': positions.to_dict('records') if not positions.empty else [],
                'pnl_24h': daily_pnl,
                'risk_metrics': risk_metrics,
                'exposure_metrics': exposure_metrics,
                'performance': performance,
                'balances': balances.to_dict('records') if not balances.empty else [],
                'timestamp': datetime.now(pytz.UTC)
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating portfolio metrics: {str(e)}")
            return {}
    
    def get_daily_pnl(self) -> float:
        """Calculate 24-hour profit/loss."""
        try:
            trades = self.account.get_trade_history(
                start_time=datetime.now() - timedelta(days=1)
            )
            if trades.empty:
                return 0.0
                
            return float(trades['pnl'].sum() if 'pnl' in trades.columns else 0.0)
            
        except Exception as e:
            self.logger.error(f"Error calculating daily PnL: {str(e)}")
            return 0.0
    
    def _calculate_risk_metrics(self) -> dict:
        """
        Calculate various risk metrics for the portfolio.
        """
        try:
            # Get positions and calculate exposures
            positions = self.account.get_position_exposure()
            total_value = self.account._calculate_total_balance_usdt()
            
            if total_value == 0:
                return {
                    'largest_position_pct': 0,
                    'concentration_score': 0,
                    'leverage_ratio': 0,
                    'margin_utilization': 0,
                    'risk_level': 'low'
                }
            
            # Calculate largest position as percentage
            largest_position_pct = (positions['value_usdt'].max() / total_value * 100 
                                  if not positions.empty else 0)
            
            # Calculate concentration score (Herfindahl-Hirschman Index)
            if not positions.empty:
                weights = positions['value_usdt'] / total_value
                concentration_score = (weights ** 2).sum()
            else:
                concentration_score = 0
                
            # Calculate leverage and margin metrics
            account_info = self.account.client.get_account()
            total_margin = float(account_info.get('totalMarginBalance', 0))
            used_margin = float(account_info.get('totalInitialMargin', 0))
            
            leverage_ratio = total_value / total_margin if total_margin > 0 else 1
            margin_utilization = (used_margin / total_margin * 100) if total_margin > 0 else 0
            
            # Determine risk level
            risk_level = self._determine_risk_level(
                largest_position_pct,
                concentration_score,
                leverage_ratio,
                margin_utilization
            )
            
            return {
                'largest_position_pct': largest_position_pct,
                'concentration_score': concentration_score,
                'leverage_ratio': leverage_ratio,
                'margin_utilization': margin_utilization,
                'risk_level': risk_level
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating risk metrics: {str(e)}")
            return {
                'largest_position_pct': 0,
                'concentration_score': 0,
                'leverage_ratio': 0,
                'margin_utilization': 0,
                'risk_level': 'unknown'
            }
    
    def _calculate_exposure_metrics(self, positions: pd.DataFrame, total_value: float) -> dict:
        """
        Calculate position exposure metrics.
        """
        try:
            if positions.empty or total_value == 0:
                return {
                    'net_exposure': 0,
                    'gross_exposure': 0,
                    'long_exposure': 0,
                    'short_exposure': 0,
                    'exposure_distribution': {}
                }
                
            # Calculate exposures
            long_positions = positions[positions['quantity'] > 0]
            short_positions = positions[positions['quantity'] < 0]
            
            long_exposure = long_positions['value_usdt'].sum() / total_value * 100
            short_exposure = abs(short_positions['value_usdt'].sum()) / total_value * 100
            
            net_exposure = long_exposure - short_exposure
            gross_exposure = long_exposure + short_exposure
            
            # Calculate exposure distribution
            exposure_distribution = positions.set_index('symbol')['exposure_pct'].to_dict()
            
            return {
                'net_exposure': net_exposure,
                'gross_exposure': gross_exposure,
                'long_exposure': long_exposure,
                'short_exposure': short_exposure,
                'exposure_distribution': exposure_distribution
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating exposure metrics: {str(e)}")
            return {
                'net_exposure': 0,
                'gross_exposure': 0,
                'long_exposure': 0,
                'short_exposure': 0,
                'exposure_distribution': {}
            }
    
    def _determine_risk_level(self,
                             largest_position_pct: float,
                             concentration_score: float,
                             leverage_ratio: float,
                             margin_utilization: float) -> str:
        """
        Determine overall risk level based on various metrics.
        """
        try:
            risk_score = 0
            
            # Position size risk
            if largest_position_pct > 50:
                risk_score += 3
            elif largest_position_pct > 30:
                risk_score += 2
            elif largest_position_pct > 20:
                risk_score += 1
                
            # Leverage risk
            if leverage_ratio > 3:
                risk_score += 3
            elif leverage_ratio > 2:
                risk_score += 2
            elif leverage_ratio > 1.5:
                risk_score += 1
                
            # Margin utilization risk
            if margin_utilization > 80:
                risk_score += 3
            elif margin_utilization > 60:
                risk_score += 2
            elif margin_utilization > 40:
                risk_score += 1
                
            # Determine final risk level
            if risk_score >= 8:
                return 'very_high'
            elif risk_score >= 6:
                return 'high'
            elif risk_score >= 4:
                return 'medium'
            elif risk_score >= 2:
                return 'low'
            else:
                return 'very_low'
                
        except Exception as e:
            self.logger.error(f"Error determining risk level: {str(e)}")
            return 'unknown'
                
            # Concentration risk
            if concentration_score > 0.5:
                risk_score += 3
            elif concentration_score > 0.3:
                risk_score += 2
            elif concentration_score > 0.2:
                risk_score += 1

    def _calculate_vwap(self, historical_data: pd.DataFrame) -> float:
        """
        Calculate Volume Weighted Average Price (VWAP)
    
        Args:
            historical_data: DataFrame with 'Close' and 'Volume' columns
    
        Returns:
            float: VWAP value
        """
        try:
            if historical_data is None or historical_data.empty:
                return 0.0
    
            # Calculate Price * Volume
            pv = historical_data['Close'] * historical_data['Volume']
            
            # Calculate VWAP
            vwap = pv.sum() / historical_data['Volume'].sum() if historical_data['Volume'].sum() > 0 else 0.0
            
            return float(vwap)
    
        except Exception as e:
            self.logger.error(f"Error calculating VWAP: {str(e)}")
            return 0.0
    
    def _calculate_twap(self, historical_data: pd.DataFrame) -> float:
        """
        Calculate Time Weighted Average Price (TWAP)
    
        Args:
            historical_data: DataFrame with 'Close' column
    
        Returns:
            float: TWAP value
        """
        try:
            if historical_data is None or historical_data.empty:
                return 0.0
    
            return float(historical_data['Close'].mean())
    
        except Exception as e:
            self.logger.error(f"Error calculating TWAP: {str(e)}")
            return 0.0
    
    def _estimate_market_impact(self, symbol: str, quantity: float) -> float:
        """
        Estimate market impact of an order using order book depth.
    
        Args:
            symbol: Trading pair symbol
            quantity: Order quantity
    
        Returns:
            float: Estimated price impact as a percentage
        """
        try:
            # Get order book metrics
            order_book_metrics = self.data_manager.get_order_book_metrics(symbol)
            if not order_book_metrics:
                return 0.0
    
            # Get current market data
            market_data = self.data_manager.get_market_snapshot(symbol)
            if not market_data:
                return 0.0
    
            current_price = market_data.get('price', 0)
            if current_price == 0:
                return 0.0
    
            # Calculate order value
            order_value = quantity * current_price
    
            # Get available liquidity
            bid_depth = order_book_metrics.get('bid_depth', 0)
            ask_depth = order_book_metrics.get('ask_depth', 0)
    
            # Calculate impact based on order size relative to available liquidity
            avg_depth = (bid_depth + ask_depth) / 2 if (bid_depth + ask_depth) > 0 else 1
            impact = min(order_value / avg_depth, 0.1)  # Cap at 10%
    
            return float(impact)
    
        except Exception as e:
            self.logger.error(f"Error estimating market impact: {str(e)}")
            return 0.0
    
    def _calculate_volatility(self, historical_data: pd.DataFrame, window: int = 20) -> float:
        """
        Calculate historical volatility.
    
        Args:
            historical_data: DataFrame with 'Close' prices
            window: Rolling window size for volatility calculation
    
        Returns:
            float: Annualized volatility
        """
        try:
            if historical_data is None or historical_data.empty:
                return 0.0
    
            # Calculate returns
            returns = np.log(historical_data['Close'] / historical_data['Close'].shift(1))
            
            # Calculate rolling volatility
            volatility = returns.rolling(window=window).std()
            
            # Annualize volatility (assuming minutely data)
            annualized_vol = volatility * np.sqrt(525600)  # Minutes in a year
    
            return float(annualized_vol.iloc[-1]) if not np.isnan(annualized_vol.iloc[-1]) else 0.0
    
        except Exception as e:
            self.logger.error(f"Error calculating volatility: {str(e)}")
            return 0.0

    def _get_optimal_price(self, symbol: str, side: str, quantity: float) -> float:
        """
        Calculate optimal order price considering multiple factors.
    
        Args:
            symbol: Trading pair symbol
            side: 'BUY' or 'SELL'
            quantity: Order quantity
    
        Returns:
            float: Optimal price for the order
        """
        try:
            # Get market data
            market_data = self.data_manager.get_market_snapshot(symbol)
            current_price = market_data.get('price', 0)
            if current_price == 0:
                return 0.0
    
            # Get recent historical data
            historical_data = self.data_manager.get_historical_data(
                symbol, 
                start_date=(datetime.now() - timedelta(minutes=60)).strftime('%Y-%m-%d %H:%M:%S')
            )
    
            # Calculate different price metrics
            vwap = self._calculate_vwap(historical_data)
            twap = self._calculate_twap(historical_data)
            impact = self._estimate_market_impact(symbol, quantity)
            volatility = self._calculate_volatility(historical_data)
    
            # Weight the different prices
            weights = {
                'current': 0.4,
                'vwap': 0.3,
                'twap': 0.2,
                'impact': 0.1
            }
    
            weighted_price = (
                current_price * weights['current'] +
                vwap * weights['vwap'] +
                twap * weights['twap']
            )
    
            # Adjust for side and impact
            if side == 'BUY':
                optimal_price = weighted_price * (1 + impact)
            else:
                optimal_price = weighted_price * (1 - impact)
    
            # Add volatility adjustment
            volatility_adjustment = volatility * current_price * 0.1  # 10% of volatility
            if side == 'BUY':
                optimal_price += volatility_adjustment
            else:
                optimal_price -= volatility_adjustment
    
            return float(optimal_price)
    
        except Exception as e:
            self.logger.error(f"Error calculating optimal price: {str(e)}")
            return 0.0
