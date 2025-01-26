import os
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Set, Any, Tuple
import asyncio
import pandas as pd
import numpy as np
import ccxt
import pytz
import websockets
from threading import Lock
import json
from collections import defaultdict
import uuid

# Import vectorbtpro
import vectorbtpro as vbt

# Import your existing FTSDownloader
from tjk_utils.FTSDownloader import FTSDownloader

class CryptoDataManager:
    def __init__(self, 
                 symbols: List[str],
                 base_currency: str = 'USDT',
                 data_path: str = '../data/market_data.h5',
                 historical_days: int = 60,
                 depth_levels: int = 20):

        # Initialize monitor as None - will be set later
        self.monitor = None
                    
        #super().__init__(symbols, base_currency, data_path)
    
        # Verify data initialization
        #for symbol in self.trading_pairs:
        #    if not self.has_data(symbol):
        #        self.logger.info(f"Initializing data for {symbol}...")
        #        success = self.update_data(symbol)
        #        if not success:
        #            self.logger.warning(f"Failed to initialize data for {symbol}")
        """
        Initialize the CryptoDataManager.
        
        Args:
            symbols: List of cryptocurrency symbols to track (e.g., ['BTC', 'ETH'])
            base_currency: Base currency for pairs (default: 'USDT')
            data_path: Path to store historical data
            historical_days: Number of days of historical data to maintain
            depth_levels: Number of order book levels to maintain (default: 20)
        """
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Initialize basic attributes
        self.symbols = symbols
        self.base_currency = base_currency
        self.trading_pairs = [f"{symbol}{base_currency}" for symbol in symbols]
        self.historical_days = historical_days
        self.depth_levels = depth_levels
        
        # Initialize data structures
        self.data: Dict[str, pd.DataFrame] = {}
        self.latest_prices: Dict[str, float] = {}
        self.last_update: Dict[str, datetime] = {}
        
        # Order book data structures
        self.order_books: Dict[str, Dict[str, pd.DataFrame]] = {
            pair: {'bids': pd.DataFrame(columns=['price', 'quantity']),
                   'asks': pd.DataFrame(columns=['price', 'quantity'])}
            for pair in self.trading_pairs
        }
        self.order_book_updates: Dict[str, datetime] = {}
        
        # Initialize locks for thread safety
        self.data_locks = {pair: Lock() for pair in self.trading_pairs}
        self.order_book_locks = {pair: Lock() for pair in self.trading_pairs}
        
        # Initialize FTSDownloader
        self.downloader = FTSDownloader(data_path)
        
        # Initialize ccxt exchange (will be used for real-time data later)
        self.exchange = ccxt.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'}
        })
        
        # Websocket related attributes
        self._ws_client = None
        self._ws_connected = False
        self._ws_tasks: Set[asyncio.Task] = set()
        self._current_candles: Dict[str, Dict] = defaultdict(dict)
        self._last_persisted_timestamp: Dict[str, float] = defaultdict(int)
        
        # Event to control websocket lifecycle
        self._ws_keepalive = asyncio.Event()
        
        # Initialize exchange for websocket
        self._ws_exchange = None  # Will be initialized when websocket starts
        
        # Load initial historical data
        self._initialize_historical_data()

        # Add reconnection settings
        self._max_reconnect_attempts = 5
        self._reconnect_delay = 5  # Base delay in seconds
        self._current_reconnect_attempt = 0
        self._last_message_time = defaultdict(float)
        self._heartbeat_interval = 30  # seconds

        # Initialize connection statistics with proper default values
        self._connection_stats = {
            'start_time': datetime.now(pytz.UTC),
            'total_messages': defaultdict(int),
            'reconnections': 0,
            'errors': defaultdict(int),
            'last_error': defaultdict(str),
            'message_rates': defaultdict(list),
            'latencies': defaultdict(list),
            'last_disconnect': None,
            'uptime_periods': []
        }
        self._stats_lock = Lock()
        self._message_count = defaultdict(int)
        self._last_message_count = defaultdict(int)
        self._last_rate_update = time.time()

    def _record_heartbeat(self, component: str):
            """Safely record a heartbeat."""
            if hasattr(self, 'monitor') and self.monitor is not None:
                self.monitor.record_heartbeat(component)
    
    def _initialize_historical_data(self):
        """
        Initialize historical data for all trading pairs.
        Loads data from FTSDownloader and sets up initial state.
        """
        start_date = (datetime.now() - timedelta(days=self.historical_days)).strftime('%Y-%m-%d')
        
        for pair in self.trading_pairs:
            try:
                # Load historical data using FTSDownloader
                data = self.downloader.load_data(
                    symbol=pair,
                    loader="ccxt",
                    periodicity='1m',
                    exchange='binance',
                    start_date=start_date,
                    auto_update=True
                )
                
                if data is not None:
                    # Convert to DataFrame if it's a vbt.Data object
                    if hasattr(data, 'get'):
                        df = data.get()
                    else:
                        df = data

                    with self.data_locks[pair]:
                        self.data[pair] = vbt.Data.from_data(df)
                        self.last_update[pair] = df.index[-1]
                        self.latest_prices[pair] = df['Close'].iloc[-1]
                        # Initialize last persisted timestamp
                        self._last_persisted_timestamp[pair] = df.index[-1].timestamp()
                        
                    self.logger.info(f"Successfully loaded historical data for {pair}")
                else:
                    self.logger.error(f"Failed to load historical data for {pair}")
                
            except Exception as e:
                self.logger.error(f"Error initializing historical data for {pair}: {str(e)}")

    def verify_data_integrity(self, symbol: str) -> Dict[str, Any]:
        """
        Verify data integrity for a symbol and fix any issues.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Dict containing verification results
        """
        try:
            with self.data_locks[symbol]:
                if symbol not in self.data:
                    return {'status': 'error', 'message': 'No data found for symbol'}

                data = self.data[symbol].get()
                
                # Check for duplicates
                duplicates = data.index.duplicated()
                if duplicates.any():
                    # Remove duplicates
                    data = data[~duplicates]
                    self.data[symbol] = vbt.Data.from_data(data)
                
                # Check for gaps
                expected_range = pd.date_range(
                    start=data.index[0],
                    end=data.index[-1],
                    freq='1min'
                )
                missing_times = expected_range.difference(data.index)
                
                return {
                    'status': 'success',
                    'total_candles': len(data),
                    'start_time': data.index[0],
                    'end_time': data.index[-1],
                    'duplicates_removed': duplicates.sum(),
                    'missing_candles': len(missing_times),
                    'missing_times': missing_times.tolist()
                }
                
        except Exception as e:
            return {'status': 'error', 'message': str(e)}
    
    def update_historical_data(self, pair: str) -> bool:
        """
        Update historical data for a specific trading pair.
        
        Args:
            pair: Trading pair to update
            
        Returns:
            bool: Success status of the update
        """
        try:
            success = self.downloader.update_data(
                symbol=pair,
                loader="ccxt",
                periodicity='1m',
                exchange='binance'
            )
            
            if success:
                # Reload the updated data
                data = self.downloader.load_data(
                    symbol=pair,
                    loader="ccxt",
                    periodicity='1m',
                    exchange='binance',
                    auto_update=False
                )
                
                if data is not None:
                    with self.data_locks[pair]:
                        self.data[pair] = data
                        self.last_update[pair] = data.get().index[-1]
                        self.latest_prices[pair] = data.get()['Close'].iloc[-1]
                    
                    self.logger.info(f"Successfully updated historical data for {pair}")
                    return True
            
            self.logger.error(f"Failed to update historical data for {pair}")
            return False
            
        except Exception as e:
            self.logger.error(f"Error updating historical data for {pair}: {str(e)}")
            return False
    
    def get_latest_price(self, pair: str) -> Optional[float]:
        """Get the latest price for a trading pair."""
        return self.latest_prices.get(pair)
    
    def get_historical_data(self, 
                          pair: str,
                          start_date: Optional[str] = None,
                          end_date: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        Get historical data for a trading pair within a specified date range.
        
        Args:
            pair: Trading pair
            start_date: Start date (optional)
            end_date: End date (optional)
            
        Returns:
            Optional[pd.DataFrame]: Historical data or None if not available
        """
        try:
            with self.data_locks[pair]:
                if pair not in self.data:
                    return None
                
                data = self.data[pair].get()
                
                if start_date:
                    start_ts = pd.Timestamp(start_date, tz='UTC')
                    data = data[data.index >= start_ts]
                if end_date:
                    end_ts = pd.Timestamp(end_date, tz='UTC')
                    data = data[data.index <= end_ts]
                
                return data
                
        except Exception as e:
            self.logger.error(f"Error retrieving historical data for {pair}: {str(e)}")
            return None
    
    async def _handle_kline_message(self, message: dict) -> None:
        """
        Handle incoming kline/candlestick messages from websocket.
        Only persists completed candles.
        
        Args:
            message: Kline message from websocket
        """
        try:
            self._record_heartbeat('data_websocket')
            
            kline = message.get('k', {})
            symbol = kline.get('s')  # Symbol
            interval = kline.get('i')  # Interval
            is_closed = kline.get('x', False)  # Whether the candle is closed
            
            if not all([symbol, interval]):
                return
                
            candle_data = {
                'Open': float(kline.get('o', 0)),
                'High': float(kline.get('h', 0)),
                'Low': float(kline.get('l', 0)),
                'Close': float(kline.get('c', 0)),
                'Volume': float(kline.get('v', 0)),
                'CloseTime': pd.to_datetime(kline.get('T', 0), unit='ms', utc=True)
            }
            
            # Update latest price regardless of candle completion
            with self.data_locks[symbol]:
                self.latest_prices[symbol] = candle_data['Close']
                self.logger.debug(f"Updated latest price for {symbol}: {candle_data['Close']}")
            
            # Only persist completed candles
            if is_closed:
                try:
                    await self._persist_candle(symbol, candle_data)
                except Exception as e:
                    self.logger.error(f"Error persisting candle for {symbol}: {str(e)}")
                    # Continue processing despite persistence error
                    
        except Exception as e:
            self.logger.error(f"Error handling kline message: {str(e)}")
            self.logger.debug(f"Problematic message: {message}")

    async def _persist_candle(self, symbol: str, candle_data: dict) -> None:
        """
        Persist a completed candle to storage while preserving existing data.
        
        Args:
            symbol: Trading pair symbol
            candle_data: Candle data to persist
        """
        try:
            # Create DataFrame from candle data
            df = pd.DataFrame([candle_data])
            df.set_index('CloseTime', inplace=True)
            
            # Avoid persisting duplicate candles
            timestamp = df.index[0].timestamp()
            if timestamp <= self._last_persisted_timestamp[symbol]:
                self.logger.debug(f"Skipping duplicate candle for {symbol} at {df.index[0]}")
                return
    
            # Load existing data
            existing_data = self.downloader.load_data(
                symbol=symbol,
                loader="ccxt",
                periodicity='1m',
                exchange='binance',
                auto_update=False
            )
    
            if existing_data is not None:
                # Convert existing data to DataFrame if it's a vbt.Data object
                if hasattr(existing_data, 'get'):
                    existing_df = existing_data.get()
                else:
                    existing_df = existing_data
    
                # Ensure columns match
                required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
                if not all(col in df.columns for col in required_columns):
                    self.logger.error(f"Missing required columns in new candle data for {symbol}")
                    return
                    
                if not all(col in existing_df.columns for col in required_columns):
                    self.logger.error(f"Missing required columns in existing data for {symbol}")
                    return
    
                # Combine existing data with new candle
                combined_df = pd.concat([existing_df, df])
                # Remove any duplicates based on index
                combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
                # Sort by timestamp
                combined_df.sort_index(inplace=True)
                
                # Ensure all required columns are present and in correct order
                combined_df = combined_df[required_columns]
            else:
                combined_df = df[required_columns]
    
            try:
                # Update storage using FTSDownloader
                self.downloader.persist_data(
                    symbol=symbol,
                    data=combined_df,
                    periodicity='1m',
                    loader='ccxt',
                    exchange='binance'
                )
                
                # Update local data cache
                with self.data_locks[symbol]:
                    self.data[symbol] = vbt.Data.from_data(combined_df)
                    self.last_update[symbol] = combined_df.index[-1]
                    self.latest_prices[symbol] = combined_df['Close'].iloc[-1]
                    self._last_persisted_timestamp[symbol] = timestamp
                
                self.logger.debug(f"Successfully persisted candle for {symbol} at {df.index[0]}")
                
            except Exception as e:
                self.logger.error(f"Error during persist operation for {symbol}: {str(e)}")
                raise
                
        except Exception as e:
            self.logger.error(f"Error preparing data for persistence for {symbol}: {str(e)}")
                        
    async def _handle_kline_data(self, message: dict) -> None:
        """Handle kline/candlestick data updates."""
        try:
            k = message['k']
            
            symbol = k['s']  # Trading pair symbol
            interval = k['i']  # Interval
            is_closed = k['x']  # Whether candle is closed
            
            candle_data = {
                'Open': float(k['o']),
                'High': float(k['h']),
                'Low': float(k['l']),
                'Close': float(k['c']),
                'Volume': float(k['v']),
                'CloseTime': pd.to_datetime(k['T'], unit='ms', utc=True)
            }
            
            # Update current candle data
            self._current_candles[symbol] = candle_data
            
            # Update latest price
            with self.data_locks[symbol]:
                self.latest_prices[symbol] = candle_data['Close']
            
            # Process completed candle
            if is_closed:
                await self._persist_candle(symbol, candle_data)
                self.logger.debug(f"Processed closed candle for {symbol}")
                
        except Exception as e:
            self.logger.error(f"Error handling kline data: {str(e)}")

    async def _handle_depth_data(self, message: dict) -> None:
        """Handle order book depth data updates."""
        try:
            # For spot market, depth update message has this structure:
            # {
            #   "e": "depthUpdate",     // Event type
            #   "E": 123456789,         // Event time
            #   "s": "BTCUSDT",         // Symbol
            #   "U": 157,               // First update ID
            #   "u": 160,               // Final update ID
            #   "b": [                  // Bids (price, quantity)
            #     ["0.0024","10"],
            #   ],
            #   "a": [                  // Asks (price, quantity)
            #     ["0.0026","100"],
            #   ]
            # }
            
            symbol = message['s']  # Symbol is in the same place
            
            # Create DataFrames for bids and asks
            # Convert strings to floats during DataFrame creation
            bids_update = pd.DataFrame(message['b'], columns=['price', 'quantity']).astype(float)
            asks_update = pd.DataFrame(message['a'], columns=['price', 'quantity']).astype(float)
            
            with self.order_book_locks[symbol]:
                # Update bids
                self.order_books[symbol]['bids'] = pd.concat([
                    self.order_books[symbol]['bids'], bids_update
                ]).groupby('price').sum().reset_index()
                self.order_books[symbol]['bids'] = self.order_books[symbol]['bids'][
                    self.order_books[symbol]['bids']['quantity'] > 0
                ]
                
                # Update asks
                self.order_books[symbol]['asks'] = pd.concat([
                    self.order_books[symbol]['asks'], asks_update
                ]).groupby('price').sum().reset_index()
                self.order_books[symbol]['asks'] = self.order_books[symbol]['asks'][
                    self.order_books[symbol]['asks']['quantity'] > 0
                ]
                
                # Sort order books and limit to depth levels
                self.order_books[symbol]['bids'] = self.order_books[symbol]['bids'].sort_values(
                    by='price', ascending=False
                ).head(self.depth_levels)
                self.order_books[symbol]['asks'] = self.order_books[symbol]['asks'].sort_values(
                    by='price', ascending=True
                ).head(self.depth_levels)
                
                # Update timestamp
                self.order_book_updates[symbol] = datetime.now()
                
                # Debug log the order book state
                self.logger.debug(f"Updated order book for {symbol}:")
                self.logger.debug(f"Bids: {len(self.order_books[symbol]['bids'])} levels")
                self.logger.debug(f"Asks: {len(self.order_books[symbol]['asks'])} levels")
                
        except Exception as e:
            self.logger.error(f"Error handling depth data for {symbol}: {str(e)}")
            self.logger.error(f"Message causing error: {message}")

    async def _maintain_websocket_connection(self):
        """Monitor websocket health and reconnect if needed."""
        while self._ws_keepalive.is_set():
            try:
                current_time = time.time()
                
                # Check last message time for each symbol
                for symbol in self.trading_pairs:
                    last_time = self._last_message_time[symbol]
                    if last_time > 0 and (current_time - last_time > self._heartbeat_interval * 2):
                        self.logger.warning(
                            f"No messages received for {symbol} in {self._heartbeat_interval * 2} seconds. "
                            "Triggering reconnection."
                        )
                        await self.restart_websocket()
                        break
                
                await asyncio.sleep(self._heartbeat_interval)
                
            except Exception as e:
                self.logger.error(f"Error in websocket monitor: {str(e)}")
                await asyncio.sleep(5)
    
    async def _kline_websocket_loop(self):
        """Maintain websocket connection with exponential backoff."""
        while self._ws_keepalive.is_set():
            try:
                self._current_reconnect_attempt = 0
                async with websockets.connect(self._get_websocket_url()) as websocket:
                    # Subscribe to streams
                    await self._subscribe_to_streams(websocket)
                    
                    # Start connection monitor
                    monitor_task = asyncio.create_task(self._maintain_websocket_connection())
                    
                    while self._ws_keepalive.is_set():
                        try:
                            message = await asyncio.wait_for(
                                websocket.recv(),
                                timeout=self._heartbeat_interval
                            )
                            if message:
                                data = json.loads(message)
                                # Update last message time
                                if 'k' in data and 's' in data['k']:
                                    self._last_message_time[data['k']['s']] = time.time()
                                await self._handle_websocket_message(data)
                                
                        except asyncio.TimeoutError:
                            # Send ping to keep connection alive
                            try:
                                pong_waiter = await websocket.ping()
                                await asyncio.wait_for(pong_waiter, timeout=5)
                            except:
                                raise websockets.ConnectionClosed(
                                    1000, "Ping timeout"
                                )
                                
                    monitor_task.cancel()
                    
            except Exception as e:
                self.logger.error(f"Websocket error: {str(e)}")
                
                # Implement exponential backoff
                if self._current_reconnect_attempt < self._max_reconnect_attempts:
                    delay = self._reconnect_delay * (2 ** self._current_reconnect_attempt)
                    self.logger.info(f"Reconnecting in {delay} seconds...")
                    await asyncio.sleep(delay)
                    self._current_reconnect_attempt += 1
                else:
                    self.logger.error("Max reconnection attempts reached")
                    self._ws_keepalive.clear()
                    break
                    
    async def start_websocket(self) -> None:
        """Start websocket connection for all trading pairs."""
        try:
            if self._ws_connected:
                return
                
            # Set keepalive flag
            self._ws_keepalive.set()
            self._ws_connected = True
            
            # Start main websocket loop
            task = asyncio.create_task(self._kline_websocket_loop())
            self._ws_tasks.add(task)
            task.add_done_callback(self._ws_tasks.discard)

            # Wait for initial order book data
            await asyncio.sleep(5)  
            
            self.logger.info("Started websocket connections for all pairs")
            
        except Exception as e:
            self._ws_connected = False
            self.logger.error(f"Error starting websocket: {str(e)}")
            raise

    async def stop_websocket(self) -> None:
        """
        Stop all websocket connections.
        """
        try:
            # Clear keepalive flag
            self._ws_keepalive.clear()
            
            # Cancel all websocket tasks
            for task in self._ws_tasks:
                task.cancel()
            
            # Wait for all tasks to complete
            if self._ws_tasks:
                await asyncio.gather(*self._ws_tasks, return_exceptions=True)
            
            # Close exchange connection
            if self._ws_exchange:
                await self._ws_exchange.close()
                
            self._ws_connected = False
            self.logger.info("Stopped all websocket connections")
            
        except Exception as e:
            self.logger.error(f"Error stopping websocket: {str(e)}")
            raise
    
    def get_websocket_status(self) -> Dict[str, Union[bool, int, Set[str], Dict]]:
        """
        Get current status of websocket connections.
        
        Returns:
            Dict containing websocket status information including order book status
        """
        status = {
            'connected': self._ws_connected,
            'active_tasks': len(self._ws_tasks),
            'subscribed_pairs': set(self._current_candles.keys()),
            'last_update': {
                symbol: datetime.fromtimestamp(self._last_persisted_timestamp[symbol])
                for symbol in self._last_persisted_timestamp
            },
            'order_books': {}
        }
        
        # Add order book status
        for symbol in self.trading_pairs:
            with self.order_book_locks[symbol]:
                status['order_books'][symbol] = {
                    'last_update': self.order_book_updates.get(symbol),
                    'bid_levels': len(self.order_books[symbol]['bids']),
                    'ask_levels': len(self.order_books[symbol]['asks']),
                }
                
                # Add basic metrics if order book is not empty
                if not self.order_books[symbol]['bids'].empty and not self.order_books[symbol]['asks'].empty:
                    metrics = self.get_order_book_metrics(symbol)
                    status['order_books'][symbol].update({
                        'bid_ask_spread': metrics.get('bid_ask_spread'),
                        'bid_ask_spread_pct': metrics.get('bid_ask_spread_pct')
                    })
        
        return status
    
    async def restart_websocket(self) -> None:
        """
        Restart all websocket connections.
        """
        await self.stop_websocket()
        await asyncio.sleep(1)  # Wait for connections to close
        await self.start_websocket()
        
    def _get_websocket_url(self) -> str:
        """Get appropriate websocket URL based on environment."""
        # Use testnet URL if in test mode
        if getattr(self.exchange, 'urls', {}).get('test'):
            return "wss://testnet.binance.vision/ws"  # spot testnet return "wss://stream.binancefuture.com/ws"
        return "wss://stream.binance.com:9443/ws" #return "wss://fstream.binance.com/ws"

    async def _subscribe_to_streams(self, websocket: websockets.WebSocketClientProtocol):
        """Subscribe to all required streams."""
        try:
            streams = []
            for pair in self.trading_pairs:
                pair_lower = pair.lower()
                # Add kline stream
                streams.append(f"{pair_lower}@kline_1m")
                # Add depth stream for spot
                streams.append(f"{pair_lower}@depth@100ms")  # Use regular depth stream for spot
            
            subscribe_message = {
                "method": "SUBSCRIBE",
                "params": streams,
                "id": str(uuid.uuid4())
            }
            
            self.logger.info(f"Subscribing to streams: {streams}")
            await websocket.send(json.dumps(subscribe_message))
            response = await websocket.recv()
            self.logger.info(f"Subscription response: {response}")
            
        except Exception as e:
            self.logger.error(f"Error in _subscribe_to_streams: {e}")
            raise
    
    async def check_websocket_health(self) -> bool:
        """Check health of websocket connections."""
        current_time = time.time()
        
        # Check message timestamps for all pairs
        for symbol in self.trading_pairs:
            last_message_time = self._last_message_time[symbol]
            if last_message_time > 0 and (current_time - last_message_time > self._heartbeat_interval * 2):
                self.logger.warning(f"Stale data for {symbol}")
                return False
                
        return True
    
    def get_data_quality_metrics(self, pair: str) -> Dict:
        """
        Get data quality metrics for a trading pair.
        
        Args:
            pair: Trading pair to check
            
        Returns:
            Dict containing metrics about data quality and completeness
        """
        try:
            with self.data_locks[pair]:
                if pair not in self.data:
                    return {}
                
                data = self.data[pair].get()
                
                # Calculate basic metrics
                metrics = {
                    'total_rows': len(data),
                    'start_date': data.index[0],
                    'end_date': data.index[-1],
                    'missing_values': data.isnull().sum().to_dict(),
                    'last_update': self.last_update.get(pair),
                }
                
                # Check for gaps in minutely data
                expected_minutes = pd.date_range(
                    start=data.index[0],
                    end=data.index[-1],
                    freq='1min'
                )
                missing_minutes = len(expected_minutes) - len(data)
                metrics['missing_minutes'] = missing_minutes
                metrics['completeness_ratio'] = len(data) / len(expected_minutes)
                
                return metrics
                
        except Exception as e:
            self.logger.error(f"Error calculating data quality metrics for {pair}: {str(e)}")
            return {}

    def get_order_book(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """
        Get current order book for a symbol.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Dict containing 'bids' and 'asks' DataFrames
        """
        with self.order_book_locks[symbol]:
            return {
                'bids': self.order_books[symbol]['bids'].copy(),
                'asks': self.order_books[symbol]['asks'].copy()
            }
    
    def get_order_book_metrics(self, symbol: str) -> Dict[str, float]:
        """
        Calculate key metrics from the order book.
        
        Args:
            symbol: Trading pair symbol
            
        Returns:
            Dict containing order book metrics:
                - bid_ask_spread: Current spread between best bid and ask
                - bid_ask_spread_pct: Spread as percentage of mid price
                - bid_depth: Total volume on bid side
                - ask_depth: Total volume on ask side
                - bid_ask_ratio: Ratio of bid to ask volume
                - weighted_mid_price: Volume-weighted midpoint price
                - price_impact_buy: Estimated price impact for market buy
                - price_impact_sell: Estimated price impact for market sell
                - update_time: Last order book update time
        """
        try:
            with self.order_book_locks[symbol]:
                bids = self.order_books[symbol]['bids']
                asks = self.order_books[symbol]['asks']
                
                if bids.empty or asks.empty:
                    return {}
                
                best_bid = bids['price'].iloc[0]
                best_ask = asks['price'].iloc[0]
                
                bid_depth = bids['quantity'].sum()
                ask_depth = asks['quantity'].sum()
                
                # Calculate volume-weighted midpoint
                weighted_mid = (
                    (best_bid * ask_depth + best_ask * bid_depth) / 
                    (bid_depth + ask_depth)
                )
                
                # Calculate price impact for standard volume (e.g., 1 BTC)
                standard_volume = 1.0
                price_impact_buy = self._calculate_price_impact(
                    asks, standard_volume, 'buy'
                )
                price_impact_sell = self._calculate_price_impact(
                    bids, standard_volume, 'sell'
                )
                
                return {
                    'bid_ask_spread': best_ask - best_bid,
                    'bid_ask_spread_pct': (best_ask - best_bid) / best_bid * 100,
                    'bid_depth': bid_depth,
                    'ask_depth': ask_depth,
                    'bid_ask_ratio': bid_depth / ask_depth if ask_depth > 0 else float('inf'),
                    'weighted_mid_price': weighted_mid,
                    'price_impact_buy': price_impact_buy,
                    'price_impact_sell': price_impact_sell,
                    'update_time': self.order_book_updates.get(symbol)
                }
                
        except Exception as e:
            self.logger.error(f"Error calculating order book metrics for {symbol}: {str(e)}")
            return {}
    
    def _calculate_price_impact(self, 
                              orders: pd.DataFrame, 
                              volume: float,
                              side: str) -> float:
        """
        Calculate price impact for a given volume.
        
        Args:
            orders: DataFrame containing order book side (bids or asks)
            volume: Volume to calculate impact for
            side: 'buy' or 'sell'
            
        Returns:
            float: Estimated price impact percentage
        """
        try:
            if orders.empty:
                return 0.0  # No orders, no impact
            
            remaining_volume = volume
            weighted_price = 0.0
            best_price = orders.iloc[0]['price']  # Best price is the first row
            
            # Iterate through orders to calculate weighted price
            for _, row in orders.iterrows():
                price = row['price']
                order_volume = row['quantity']
                
                if remaining_volume <= 0:
                    break
                    
                filled_volume = min(remaining_volume, order_volume)
                weighted_price += price * filled_volume
                remaining_volume -= filled_volume
            
            # If not enough liquidity, use the last available price
            if remaining_volume > 0:
                weighted_price += price * remaining_volume  # Use the last price
            
            # Calculate average price
            avg_price = weighted_price / volume
            
            # Calculate impact relative to best price
            impact = ((avg_price - best_price) / best_price * 100 * 
                     (1 if side == 'buy' else -1))
            
            return impact
            
        except Exception as e:
            self.logger.error(f"Error calculating price impact: {str(e)}")
            return 0.0
    
    def get_liquidity_analysis_old(self, symbol: str, volume_levels: List[float] = None) -> Dict:
        """
        Analyze liquidity at different volume levels.
        
        Args:
            symbol: Trading pair symbol
            volume_levels: List of volumes to analyze (default: [0.1, 0.5, 1.0, 5.0, 10.0])
            
        Returns:
            Dict containing liquidity analysis at each volume level
        """
        if volume_levels is None:
            volume_levels = [0.1, 0.5, 1.0, 5.0, 10.0]
            
        try:
            analysis = {'buy': {}, 'sell': {}}
            
            with self.order_book_locks[symbol]:
                for vol in volume_levels:
                    # Calculate buy side impact
                    analysis['buy'][vol] = {
                        'price_impact': self._calculate_price_impact(
                            self.order_books[symbol]['asks'], vol, 'buy'
                        ),
                        'volume': vol
                    }
                    
                    # Calculate sell side impact
                    analysis['sell'][vol] = {
                        'price_impact': self._calculate_price_impact(
                            self.order_books[symbol]['bids'], vol, 'sell'
                        ),
                        'volume': vol
                    }
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Error analyzing liquidity for {symbol}: {str(e)}")
            return {}

    def get_liquidity_analysis_old_2(self, symbol: str) -> Dict[str, Any]:
        """Get detailed liquidity analysis."""
        try:
            with self.order_book_locks[symbol]:
                bids = self.order_books[symbol]['bids']
                asks = self.order_books[symbol]['asks']
                
                current_price = self.latest_prices.get(symbol, 0)
                
                # Calculate liquidity metrics with safe division
                bid_liquidity = bids['quantity'].sum() if not bids.empty else 0
                ask_liquidity = asks['quantity'].sum() if not asks.empty else 0
                
                # Calculate imbalance safely
                total_liquidity = bid_liquidity + ask_liquidity
                imbalance = ((bid_liquidity - ask_liquidity) / total_liquidity 
                            if total_liquidity > 0 else 0)

                # Get best bid/ask safely
                best_bid = bids.iloc[0]['price'] if not bids.empty else current_price
                best_ask = asks.iloc[0]['price'] if not asks.empty else current_price
                spread = best_ask - best_bid if (best_ask > 0 and best_bid > 0) else 0

                return {
                    'current_price': current_price,
                    'bid_liquidity': bid_liquidity,
                    'ask_liquidity': ask_liquidity,
                    'imbalance': imbalance,
                    'spread': spread,
                    'last_update': datetime.now(pytz.UTC)
                }

        except Exception as e:
            self.logger.error(f"Error analyzing liquidity for {symbol}: {e}")
            return {
                'current_price': 0,
                'bid_liquidity': 0,
                'ask_liquidity': 0,
                'imbalance': 0,
                'spread': 0,
                'last_update': datetime.now(pytz.UTC)
            }

    def get_liquidity_analysis(self, symbol: str) -> Dict[str, Any]:
        """Get detailed liquidity analysis."""
        try:
            with self.order_book_locks[symbol]:
                bids = self.order_books[symbol]['bids']
                asks = self.order_books[symbol]['asks']
                
                current_price = self.latest_prices.get(symbol, 0)
                
                # Calculate liquidity metrics with safe division
                bid_liquidity = bids['quantity'].sum() if not bids.empty else 0
                ask_liquidity = asks['quantity'].sum() if not asks.empty else 0
                
                # Calculate imbalance with safe division
                total_liquidity = bid_liquidity + ask_liquidity
                if total_liquidity > 0:
                    imbalance = (bid_liquidity - ask_liquidity) / total_liquidity
                else:
                    imbalance = 0
                    self.logger.warning(f"Total liquidity is zero for {symbol}")
    
                # Get best bid/ask safely
                best_bid = bids.iloc[0]['price'] if not bids.empty else current_price
                best_ask = asks.iloc[0]['price'] if not asks.empty else current_price
                spread = best_ask - best_bid if (best_ask > 0 and best_bid > 0) else 0
    
                return {
                    'current_price': current_price,
                    'bid_liquidity': bid_liquidity,
                    'ask_liquidity': ask_liquidity,
                    'imbalance': imbalance,
                    'spread': spread,
                    'last_update': datetime.now(pytz.UTC)
                }
    
        except Exception as e:
            self.logger.error(f"Error analyzing liquidity for {symbol}: {e}")
            return {
                'current_price': 0,
                'bid_liquidity': 0,
                'ask_liquidity': 0,
                'imbalance': 0,
                'spread': 0,
                'last_update': datetime.now(pytz.UTC)
            }
        
    def get_market_depth_imbalance(self, symbol: str, levels: int = 5) -> Dict[str, float]:
        """
        Calculate market depth imbalance metrics.
        
        Args:
            symbol: Trading pair symbol
            levels: Number of price levels to consider
            
        Returns:
            Dict containing imbalance metrics
        """
        try:
            with self.order_book_locks[symbol]:
                bids = self.order_books[symbol]['bids'].head(levels)
                asks = self.order_books[symbol]['asks'].head(levels)
                
                bid_volume = bids['quantity'].sum()
                ask_volume = asks['quantity'].sum()
                
                total_volume = bid_volume + ask_volume
                
                return {
                    'bid_volume': bid_volume,
                    'ask_volume': ask_volume,
                    'total_volume': total_volume,
                    'bid_percentage': (bid_volume / total_volume * 100) if total_volume > 0 else 50,
                    'ask_percentage': (ask_volume / total_volume * 100) if total_volume > 0 else 50,
                    'imbalance_ratio': (bid_volume - ask_volume) / (bid_volume + ask_volume)
                    if total_volume > 0 else 0
                }
                
        except Exception as e:
            self.logger.error(f"Error calculating depth imbalance for {symbol}: {str(e)}")
            return {}

    def get_market_snapshot(self, symbol: str) -> Dict[str, Any]:
        """
        Get comprehensive market snapshot for a symbol.
        """
        try:
            with self.data_locks[symbol]:
                if symbol not in self.data:
                    return {}

                current_data = self.data[symbol].get()
                latest_price = self.latest_prices[symbol]
                last_candle = current_data.iloc[-1]

                # Calculate basic metrics
                change_24h = ((latest_price - current_data['Close'].iloc[-1440]) / 
                            current_data['Close'].iloc[-1440] * 100)  # 1440 minutes in 24h
                
                volume_24h = current_data['Volume'].tail(1440).sum()
                
                # Calculate volatility
                returns = current_data['Close'].pct_change()
                volatility = returns.std() * np.sqrt(1440) * 100  # Annualized volatility

                # Get order book metrics
                orderbook_metrics = self.get_order_book_metrics(symbol)

                return {
                    'price': latest_price,
                    'change_24h': change_24h,
                    'volume_24h': volume_24h,
                    'volatility': volatility,
                    'high_24h': current_data['High'].tail(1440).max(),
                    'low_24h': current_data['Low'].tail(1440).min(),
                    'orderbook': orderbook_metrics,
                    'timestamp': datetime.now(pytz.UTC)  # Use pytz.UTC instead of 'UTC' string
                }

        except Exception as e:
            self.logger.error(f"Error getting market snapshot for {symbol}: {str(e)}")
            return {}

    def get_volume_profile(self, symbol: str, periods: int = 1440) -> Dict[str, Any]:
        """
        Get volume profile analysis for price levels.
        """
        try:
            with self.data_locks[symbol]:
                if symbol not in self.data:
                    return {}

                data = self.data[symbol].get().tail(periods)
                
                # Create price bins
                price_range = pd.interval_range(
                    start=data['Low'].min(),
                    end=data['High'].max(),
                    periods=50
                )
                
                # Calculate volume profile
                volume_profile = (
                    pd.cut(data['Close'], bins=price_range)
                    .value_counts()
                    .sort_index()
                )
                
                # Find POC (Point of Control)
                poc_level = volume_profile.idxmax()
                
                return {
                    'volume_profile': volume_profile.to_dict(),
                    'poc_price': poc_level.mid,
                    'value_area_high': volume_profile.tail(10).index[-1].right,
                    'value_area_low': volume_profile.head(10).index[0].left
                }

        except Exception as e:
            self.logger.error(f"Error calculating volume profile for {symbol}: {str(e)}")
            return {}

    def get_liquidity_analysis_old_3(self, symbol: str) -> Dict[str, Any]:
        """
        Get detailed liquidity analysis.
        """
        try:
            with self.order_book_locks[symbol]:
                bids = self.order_books[symbol]['bids']
                asks = self.order_books[symbol]['asks']
                
                current_price = self.latest_prices[symbol]
                
                # Calculate liquidity metrics
                bid_liquidity = bids['quantity'].sum()
                ask_liquidity = asks['quantity'].sum()
                
                # Calculate imbalance
                imbalance = (bid_liquidity - ask_liquidity) / (bid_liquidity + ask_liquidity)
                
                # Calculate price impact for different volumes
                price_impacts = {}
                for volume in [0.1, 0.5, 1.0, 5.0, 10.0]:
                    price_impacts[volume] = {
                        'buy': self._calculate_price_impact(asks, volume, 'buy'),
                        'sell': self._calculate_price_impact(bids, volume, 'sell')
                    }

                return {
                    'current_price': current_price,
                    'bid_liquidity': bid_liquidity,
                    'ask_liquidity': ask_liquidity,
                    'imbalance': imbalance,
                    'price_impacts': price_impacts,
                    'spread': asks.iloc[0]['price'] - bids.iloc[0]['price'] if not bids.empty and not asks.empty else None
                }

        except Exception as e:
            self.logger.error(f"Error analyzing liquidity for {symbol}: {str(e)}")
            return {}

    def get_liquidity_analysis(self, symbol: str) -> Dict[str, Any]:
        """Get detailed liquidity analysis."""
        try:
            with self.order_book_locks[symbol]:
                bids = self.order_books[symbol]['bids']
                asks = self.order_books[symbol]['asks']
                
                current_price = self.latest_prices.get(symbol, 0)
                
                # Calculate liquidity metrics with safe division
                bid_liquidity = bids['quantity'].sum() if not bids.empty else 0
                ask_liquidity = asks['quantity'].sum() if not asks.empty else 0
                
                # Calculate imbalance with safe division
                total_liquidity = bid_liquidity + ask_liquidity
                if total_liquidity > 0:
                    imbalance = (bid_liquidity - ask_liquidity) / total_liquidity
                else:
                    imbalance = 0
                    self.logger.warning(f"Total liquidity is zero for {symbol}")
    
                # Get best bid/ask safely
                best_bid = bids.iloc[0]['price'] if not bids.empty else current_price
                best_ask = asks.iloc[0]['price'] if not asks.empty else current_price
                spread = best_ask - best_bid if (best_ask > 0 and best_bid > 0) else 0
    
                return {
                    'current_price': current_price,
                    'bid_liquidity': bid_liquidity,
                    'ask_liquidity': ask_liquidity,
                    'imbalance': imbalance,
                    'spread': spread,
                    'last_update': datetime.now(pytz.UTC)
                }
    
        except Exception as e:
            self.logger.error(f"Error analyzing liquidity for {symbol}: {e}")
            return {
                'current_price': 0,
                'bid_liquidity': 0,
                'ask_liquidity': 0,
                'imbalance': 0,
                'spread': 0,
                'last_update': datetime.now(pytz.UTC)
            }

    def get_relative_strength(self, base_symbol: str = 'BTC', lookback_periods: int = 1440) -> Dict[str, Any]:
        """
        Calculate relative strength of all pairs compared to base symbol.
        
        Args:
            base_symbol: Base symbol for comparison (default: 'BTC')
            lookback_periods: Number of minutes to look back (default: 1440 = 24h)
            
        Returns:
            Dict containing relative strength metrics and rankings
        """
        try:
            base_pair = f"{base_symbol}{self.base_currency}"
            results = {}
            
            # First get base symbol returns
            with self.data_locks[base_pair]:
                if base_pair not in self.data:
                    self.logger.error(f"Base symbol {base_pair} data not found")
                    return {}
                    
                base_data = self.data[base_pair].get()
                if base_data.empty:
                    return {}
                    
                # Calculate base returns
                base_data = base_data.tail(lookback_periods)
                base_returns = base_data['Close'].pct_change().fillna(0)
                base_cumulative_return = (1 + base_returns).cumprod().iloc[-1] - 1
                
            # Calculate relative strength for each symbol
            relative_metrics = {}
            
            for symbol in self.trading_pairs:
                if symbol == base_pair:
                    continue
                    
                try:
                    with self.data_locks[symbol]:
                        if symbol not in self.data:
                            continue
                            
                        symbol_data = self.data[symbol].get()
                        if symbol_data.empty:
                            continue
                            
                        # Get matching timeframe data
                        symbol_data = symbol_data.tail(lookback_periods)
                        symbol_returns = symbol_data['Close'].pct_change().fillna(0)
                        symbol_cumulative_return = (1 + symbol_returns).cumprod().iloc[-1] - 1
                        
                        # Calculate correlation with base
                        correlation = symbol_returns.corr(base_returns)
                        
                        # Calculate various relative strength metrics
                        relative_metrics[symbol] = {
                            'relative_strength': symbol_cumulative_return / base_cumulative_return 
                                if base_cumulative_return != 0 else float('inf'),
                            'correlation': correlation,
                            'cumulative_return': symbol_cumulative_return * 100,  # Convert to percentage
                            'volatility': symbol_returns.std() * np.sqrt(1440) * 100,  # Annualized volatility
                            'recent_momentum': (symbol_data['Close'].iloc[-1] / symbol_data['Close'].iloc[-20] - 1) * 100  # 20-period momentum
                        }
                        
                except Exception as e:
                    self.logger.error(f"Error calculating relative strength for {symbol}: {str(e)}")
                    continue
            
            if not relative_metrics:
                return {}
                
            # Create rankings
            rankings = {}
            metrics = ['relative_strength', 'correlation', 'cumulative_return', 'volatility', 'recent_momentum']
            
            for metric in metrics:
                values = [metrics[metric] for metrics in relative_metrics.values()]
                sorted_symbols = sorted(relative_metrics.keys(), 
                                     key=lambda x: relative_metrics[x][metric],
                                     reverse=True)
                rankings[metric] = {
                    symbol: rank + 1 
                    for rank, symbol in enumerate(sorted_symbols)
                }
            
            # Calculate composite score (lower is better)
            composite_scores = {}
            for symbol in relative_metrics:
                composite_scores[symbol] = sum(rankings[metric][symbol] for metric in metrics)
            
            # Add the rankings and composite score to the results
            results = {
                'base_symbol': base_pair,
                'lookback_periods': lookback_periods,
                'base_performance': {
                    'cumulative_return': base_cumulative_return * 100,
                    'volatility': base_returns.std() * np.sqrt(1440) * 100
                },
                'relative_metrics': relative_metrics,
                'rankings': rankings,
                'composite_scores': composite_scores,
                'timestamp': datetime.now(pytz.UTC)
            }
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in get_relative_strength: {str(e)}")
            return {}
            
    def get_strength_summary(self, base_symbol: str = 'BTC') -> pd.DataFrame:
        """
        Get a summarized DataFrame of relative strength metrics.
        
        Args:
            base_symbol: Base symbol for comparison
            
        Returns:
            DataFrame with relative strength metrics
        """
        try:
            strength_data = self.get_relative_strength(base_symbol)
            if not strength_data:
                return pd.DataFrame()
                
            # Create DataFrame from relative metrics
            metrics = strength_data['relative_metrics']
            df = pd.DataFrame.from_dict(metrics, orient='index')
            
            # Add composite scores
            df['composite_score'] = df.index.map(strength_data['composite_scores'])
            
            # Add rankings
            for metric in strength_data['rankings']:
                df[f'{metric}_rank'] = df.index.map(strength_data['rankings'][metric])
            
            # Sort by composite score
            df = df.sort_values('composite_score')
            
            # Format numeric columns
            float_cols = ['relative_strength', 'correlation', 'cumulative_return', 
                         'volatility', 'recent_momentum']
            for col in float_cols:
                df[col] = df[col].round(4)
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error in get_strength_summary: {str(e)}")
            return pd.DataFrame()

    def get_connection_statistics(self) -> Dict[str, Any]:
        """
        Get detailed connection statistics and performance metrics.
        """
        try:
            with self._stats_lock:
                current_time = datetime.now(pytz.UTC)
                uptime = (current_time - self._connection_stats['start_time']).total_seconds()
                
                stats = {
                    'general': {
                        'start_time': self._connection_stats['start_time'],
                        'uptime': uptime,
                        'total_reconnections': self._connection_stats['reconnections'],
                        'last_disconnect': self._connection_stats['last_disconnect'],
                        'connection_status': self._ws_connected
                    },
                    'messages': {
                        symbol: {
                            'total_messages': self._connection_stats['total_messages'][symbol],
                            'message_rate': self._calculate_current_rate(symbol),
                            'last_message_age': time.time() - self._last_message_time[symbol] 
                                if self._last_message_time[symbol] > 0 else float('inf')
                        }
                        for symbol in self.trading_pairs
                    },
                    'performance': {
                        symbol: {
                            'avg_latency': self._calculate_avg_latency(symbol),
                            'max_latency': max(self._connection_stats['latencies'][symbol]) 
                                if self._connection_stats['latencies'][symbol] else 0,
                            'message_rate_stats': self._calculate_rate_stats(symbol)
                        }
                        for symbol in self.trading_pairs
                    },
                    'errors': {
                        'total_by_type': dict(self._connection_stats['errors']),
                        'last_error': dict(self._connection_stats['last_error'])
                    },
                    'websocket': {
                        'active_tasks': len(self._ws_tasks),
                        'reconnect_attempt': self._current_reconnect_attempt,
                        'keepalive_status': self._ws_keepalive.is_set()
                    }
                }

                return stats

        except Exception as e:
            self.logger.error(f"Error getting connection statistics: {str(e)}")
            return {}

    def _calculate_message_rate(self, symbol: str) -> float:
        """Calculate current message rate for a symbol (messages per second)."""
        try:
            rates = self._connection_stats['message_rates'][symbol]
            if not rates:
                return 0.0
            
            # Calculate rate over last minute
            recent_rates = rates[-60:] if len(rates) > 60 else rates
            return sum(recent_rates) / len(recent_rates)
            
        except Exception as e:
            self.logger.error(f"Error calculating message rate for {symbol}: {str(e)}")
            return 0.0

    def _calculate_avg_latency(self, symbol: str) -> float:
        """Calculate average latency for a symbol (milliseconds)."""
        try:
            latencies = self._connection_stats['latencies'][symbol]
            if not latencies:
                return 0.0
            
            # Calculate average over last 100 messages
            recent_latencies = latencies[-100:] if len(latencies) > 100 else latencies
            return sum(recent_latencies) / len(recent_latencies)
            
        except Exception as e:
            self.logger.error(f"Error calculating average latency for {symbol}: {str(e)}")
            return 0.0

    def _calculate_rate_stats(self, symbol: str) -> Dict[str, float]:
        """Calculate detailed message rate statistics."""
        try:
            rates = self._connection_stats['message_rates'][symbol]
            if not rates:
                return {'min': 0, 'max': 0, 'avg': 0}
            
            return {
                'min': min(rates),
                'max': max(rates),
                'avg': sum(rates) / len(rates)
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating rate stats for {symbol}: {str(e)}")
            return {'min': 0, 'max': 0, 'avg': 0}

    async def _handle_websocket_message_old(self, message: dict) -> None:
        """
        Handle incoming websocket messages with statistics tracking.
        """
        try:
            message_time = time.time()
            
            # Update basic message statistics
            with self._stats_lock:
                # Track message receipt
                if isinstance(message, dict):
                    symbol = None
                    
                    # Handle kline messages
                    if 'k' in message:
                        symbol = message['k'].get('s')
                    
                    # Handle depth messages
                    elif 'e' in message and message['e'] == 'depthUpdate':
                        symbol = message.get('s')  # Spot market format uses 's' for symbol
                    
                    if symbol:
                        self._connection_stats['total_messages'][symbol] += 1
                        self._message_count[symbol] += 1
                        self._last_message_time[symbol] = message_time
                        
                        # Calculate latency if server timestamp is available
                        if 'E' in message:
                            server_time = message['E'] / 1000  # Convert to seconds
                            latency = message_time - server_time
                            self._connection_stats['latencies'][symbol].append(latency * 1000)  # Store in ms
    
            # Update message rates periodically (every second)
            if message_time - self._last_rate_update >= 1.0:
                self._update_message_rates()
                self._last_rate_update = message_time
    
            # Process the actual message data
            if 'k' in message:
                await self._handle_kline_message(message)
            elif 'e' in message and message['e'] == 'depthUpdate':
                await self._handle_depth_data(message)  # Pass the full message
            
        except Exception as e:
            self.logger.error(f"Error handling websocket message: {str(e)}")
            with self._stats_lock:
                error_type = type(e).__name__
                self._connection_stats['errors'][error_type] += 1
                self._connection_stats['last_error'][error_type] = str(e)

    async def _handle_websocket_message(self, message: dict) -> None:
        """Process incoming Binance spot market websocket messages."""
        try:
            self._record_heartbeat('data_websocket')
            
            if 'k' in message:  # Kline/candlestick data
                await self._handle_kline_message(message)
                symbol = message['k']['s']
                self._last_message_time[symbol] = time.time()
                
            elif 'e' in message and message['e'] == 'depthUpdate':  # Depth update
                await self._handle_depth_data(message)
                symbol = message['s']
                self._last_message_time[symbol] = time.time()
    
            elif 'data' in message:  # Aggregated trades
                symbol = message['data']['s']
                self._last_message_time[symbol] = time.time()
                
            self.logger.debug(f"Processed message for {symbol if 'symbol' in locals() else 'unknown'}")
    
        except Exception as e:
            self.logger.error(f"Error handling spot market message: {e}")
            self.logger.debug(f"Message causing error: {message}")
    
    def _update_message_rate(self, symbol: str) -> None:
        """Update message rate statistics."""
        rates = self._connection_stats['message_rates'][symbol]
        
        # Calculate rate over last second
        current_time = time.time()
        recent_messages = sum(1 for t in self._last_message_time.values() 
                            if current_time - t <= 1.0)
        rates.append(recent_messages)


    def _update_message_rates(self) -> None:
        """Update message rates for all symbols."""
        current_time = time.time()
        with self._stats_lock:
            for symbol in self.trading_pairs:
                # Calculate messages per second
                current_count = self._message_count[symbol]
                messages_since_last = current_count - self._last_message_count[symbol]
                self._last_message_count[symbol] = current_count
                
                # Store the rate
                self._connection_stats['message_rates'][symbol].append(messages_since_last)
                
                # Trim stored rates (keep last hour)
                if len(self._connection_stats['message_rates'][symbol]) > 3600:
                    self._connection_stats['message_rates'][symbol] = \
                        self._connection_stats['message_rates'][symbol][-3600:]
                
                # Trim stored latencies (keep last hour)
                if len(self._connection_stats['latencies'][symbol]) > 3600:
                    self._connection_stats['latencies'][symbol] = \
                        self._connection_stats['latencies'][symbol][-3600:]

    def _trim_statistics(self, symbol: str) -> None:
        """Trim stored statistics to prevent memory growth."""
        max_stored = 3600  # Store up to 1 hour of stats
        
        if len(self._connection_stats['latencies'][symbol]) > max_stored:
            self._connection_stats['latencies'][symbol] = self._connection_stats['latencies'][symbol][-max_stored:]
            
        if len(self._connection_stats['message_rates'][symbol]) > max_stored:
            self._connection_stats['message_rates'][symbol] = self._connection_stats['message_rates'][symbol][-max_stored:]

    def _calculate_current_rate(self, symbol: str) -> float:
        """Calculate current message rate for a symbol."""
        rates = self._connection_stats['message_rates'][symbol]
        if not rates:
            return 0.0
        # Return average of last 5 seconds
        recent_rates = rates[-5:] if len(rates) >= 5 else rates
        return sum(recent_rates) / len(recent_rates)

    def _calculate_avg_latency(self, symbol: str) -> float:
        """Calculate average latency for a symbol."""
        latencies = self._connection_stats['latencies'][symbol]
        if not latencies:
            return 0.0
        recent_latencies = latencies[-100:] if len(latencies) > 100 else latencies
        return sum(recent_latencies) / len(recent_latencies)

    def _calculate_rate_stats(self, symbol: str) -> Dict[str, float]:
        """Calculate message rate statistics for a symbol."""
        rates = self._connection_stats['message_rates'][symbol]
        if not rates:
            return {'min': 0.0, 'max': 0.0, 'avg': 0.0}
        return {
            'min': min(rates),
            'max': max(rates),
            'avg': sum(rates) / len(rates)
        }

    async def _check_data_flow(self):
        """Monitor data flow health."""
        while self._ws_connected:
            try:
                for symbol in self.trading_pairs:
                    last_time = self._last_message_time.get(symbol, 0)
                    if time.time() - last_time > 5:  # No data for 5 seconds
                        self.logger.warning(f"No data received for {symbol} in last 5 seconds")
                
                # Log websocket stats every minute
                if time.time() % 60 < 1:
                    stats = await self.get_connection_statistics()
                    self.logger.info(f"WebSocket Stats: {stats}")
                    
            except Exception as e:
                self.logger.error(f"Error checking data flow: {e}")
            
            await asyncio.sleep(5)
