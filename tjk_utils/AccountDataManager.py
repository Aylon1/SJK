import logging
from typing import Dict, Optional, Union, Callable, List, DefaultDict
import pandas as pd
from datetime import datetime, timedelta
import asyncio
from binance import Client, AsyncClient
from binance.streams import BinanceSocketManager
from decimal import Decimal
import nest_asyncio
from threading import Lock
from collections import defaultdict
import time
import pytz

# Apply nest_asyncio to make async work in Jupyter
nest_asyncio.apply()

class BinanceSpotAccount:
    """
    Handles Binance spot account interactions with real-time balance updates,
    order tracking, and trade monitoring.
    """

    def __init__(self, api_key: str, api_secret: str, use_testnet: bool = True, tracked_currencies: Optional[List[str]] = None):
        """
        Initialize Binance spot account connection.

        Args:
            api_key: Binance API key
            api_secret: Binance API secret
            use_testnet: Whether to use testnet (default: True)
            tracked_currencies: List of currencies to track (e.g., ['BTC', 'ETH', 'LTC', 'RVN'])
        """
        # Add missing attribute
        self._last_message_time = {}
        self._ws_connected = False
        
        # Initialize monitor as None - will be set later
        self.monitor = None
                    
        # Setup logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # API credentials and settings
        self.api_key = api_key
        self.api_secret = api_secret
        self.use_testnet = use_testnet

        # Tracked currencies
        self.tracked_currencies = tracked_currencies or []

        # Initialize clients to None
        self.client = None
        self.async_client = None
        self.socket_manager = None
        self.user_ws = None

        # Balance tracking
        self.balances: Dict[str, Dict[str, Decimal]] = {}
        self._balance_callbacks: List[Callable] = []

        # Order and trade tracking
        self.orders: DefaultDict[str, Dict] = defaultdict(dict)
        self.trades: DefaultDict[str, List] = defaultdict(list)
        self.order_updates: DefaultDict[str, List] = defaultdict(list)

        # Locks for thread safety
        self.balances_lock = Lock()
        self.orders_lock = Lock()
        self.trades_lock = Lock()

        # Callbacks
        self._order_callbacks: List[Callable] = []
        self._trade_callbacks: List[Callable] = []

        # Status tracking
        self.is_running = False

        self._tasks = set()  # Registry for running tasks



        # Initialize connection
        self._initialize_client()

    async def _start_task(self, coro):
        """Start a task and add it to the registry."""
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task
    
    def _record_heartbeat(self, component: str):
            """Safely record a heartbeat."""
            if hasattr(self, 'monitor') and self.monitor is not None:
                self.monitor.record_heartbeat(component)

    async def _check_connection_old(self) -> bool:
        """Check if account connection is still valid."""
        try:
            # Test API connection with a simple request
            # Use account info as it's lightweight and always available
            account_info = await self.async_client.get_account()
            
            # Check if we can access basic account info
            if account_info and 'canTrade' in account_info:
                self._record_heartbeat('account_connection')
                return True
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking account connection: {e}")
            return False

    async def _check_connection(self) -> bool:
        try:
            if not self.async_client:
                return False
            account_info = await self.async_client.get_account()
            return bool(account_info and 'canTrade' in account_info)
        except Exception as e:
            self.logger.error(f"Error checking account connection: {e}")
            return False
    
    async def check_connection_status(self) -> Dict:
        """Get detailed connection status."""
        try:
            is_connected = await self._check_connection()
            
            # Get websocket status if available
            ws_status = {
                'connected': self._ws_connected,
                'last_message': self._last_message_time
            } if hasattr(self, '_ws_connected') else {'connected': False}
            
            return {
                'api_connected': is_connected,
                'websocket': ws_status,
                'is_running': self.is_running,
                'last_update': datetime.now(pytz.UTC)
            }
            
        except Exception as e:
            self.logger.error(f"Error getting connection status: {e}")
            return {
                'api_connected': False,
                'websocket': {'connected': False},
                'is_running': False,
                'error': str(e)
            }
    
    def _initialize_client(self) -> None:
        """Initialize the Binance client and fetch initial data."""
        try:
            # Initialize REST client
            self.client = Client(
                self.api_key,
                self.api_secret,
                testnet=self.use_testnet
            )

            # Fetch initial balances
            self._update_initial_balances()

            # Fetch open orders
            self._update_initial_orders()

            self.logger.info("Successfully initialized Binance client")

        except Exception as e:
            self.logger.error(f"Failed to initialize Binance client: {str(e)}")
            raise

    def _update_initial_balances(self) -> None:
        """Fetch and store initial account balances."""
        try:
            account_info = self.client.get_account()

            with self.balances_lock:
                # Process and store balances
                for balance in account_info['balances']:
                    asset = balance['asset']
                    if self.tracked_currencies and asset not in self.tracked_currencies:
                        continue  # Skip untracked currencies

                    self.balances[asset] = {
                        'free': Decimal(balance['free']),
                        'locked': Decimal(balance['locked'])
                    }

        except Exception as e:
            self.logger.error(f"Error fetching initial balances: {str(e)}")
            raise

    def _update_initial_orders(self) -> None:
        """Fetch and store initial open orders."""
        try:
            open_orders = self.client.get_open_orders()

            with self.orders_lock:
                for order in open_orders:
                    symbol = order['symbol']
                    base_currency = symbol.replace('USDT', '')  # Assuming USDT pairs

                    if self.tracked_currencies and base_currency not in self.tracked_currencies:
                        continue  # Skip untracked currencies

                    order_id = order['orderId']
                    self.orders[symbol][order_id] = {
                        'orderId': order_id,
                        'symbol': symbol,
                        'status': order['status'],
                        'side': order['side'],
                        'type': order['type'],
                        'quantity': float(order['origQty']),
                        'price': float(order['price']),
                        'updateTime': pd.Timestamp(order['updateTime'], unit='ms'),
                        'executedQty': float(order['executedQty']),
                        'cumQuote': float(order['cummulativeQuoteQty']),
                        'lastPrice': None
                    }

        except Exception as e:
            self.logger.error(f"Error fetching initial orders: {str(e)}")
            raise

    async def _initialize_user_socket_old(self) -> None:
        """Initialize user data websocket connection."""
        try:
            # Initialize async client
            self.async_client = await AsyncClient.create(
                self.api_key,
                self.api_secret,
                testnet=self.use_testnet
            )
    
            # Initialize socket manager
            self.socket_manager = BinanceSocketManager(self.async_client)
    
            # Start user data stream
            self.user_ws = self.socket_manager.user_socket()
    
            # Start listening for messages
            asyncio.create_task(self._user_socket_listener())
            self.logger.info("User data websocket initialized")
    
        except Exception as e:
            self.logger.error(f"Error initializing user socket: {str(e)}")
            raise

    async def _initialize_user_socket_old_2(self) -> None:
        """Initialize user data websocket connection with better error handling."""
        try:
            # Initialize async client
            self.async_client = await AsyncClient.create(
                self.api_key,
                self.api_secret,
                testnet=self.use_testnet
            )
    
            # Initialize socket manager
            self.socket_manager = BinanceSocketManager(self.async_client)
    
            # Start user data stream with error checking
            try:
                self.user_ws = self.socket_manager.user_socket()
                if not self.user_ws:
                    raise ValueError("Failed to create user socket")
            except Exception as socket_error:
                self.logger.error(f"Failed to create user socket: {str(socket_error)}")
                raise
    
            # Start listening for messages
            try:
                asyncio.create_task(self._user_socket_listener())
                self.logger.info("User data websocket initialized")
            except Exception as listener_error:
                self.logger.error(f"Failed to start socket listener: {str(listener_error)}")
                raise
    
        except Exception as e:
            self.logger.error(f"Error initializing user socket: {str(e)}")
            # Clean up resources if initialization fails
            if hasattr(self, 'async_client') and self.async_client:
                try:
                    await self.async_client.close_connection()
                except:
                    pass
            self.async_client = None
            self.socket_manager = None
            self.user_ws = None
            raise
    
    async def _initialize_user_socket(self) -> None:
        """Initialize user data websocket connection with better error handling."""
        try:
            # Initialize async client
            self.async_client = await AsyncClient.create(
                self.api_key,
                self.api_secret,
                testnet=self.use_testnet
            )
    
            # Initialize socket manager
            self.socket_manager = BinanceSocketManager(self.async_client)
    
            # Start user data stream with error checking
            try:
                self.user_ws = self.socket_manager.user_socket()
                if not self.user_ws:
                    raise ValueError("Failed to create user socket")
            except Exception as socket_error:
                self.logger.error(f"Failed to create user socket: {str(socket_error)}")
                raise
    
            # Start listening for messages
            try:
                asyncio.create_task(self._user_socket_listener())
                self._ws_connected = True  # Mark as connected
                self.logger.info("User data websocket initialized")
            except Exception as listener_error:
                self.logger.error(f"Failed to start socket listener: {str(listener_error)}")
                raise
    
        except Exception as e:
            self.logger.error(f"Error initializing user socket: {str(e)}")
            # Clean up resources if initialization fails
            if hasattr(self, 'async_client') and self.async_client:
                try:
                    await self.async_client.close_connection()
                except:
                    pass
            self.async_client = None
            self.socket_manager = None
            self.user_ws = None
            raise
        
    async def _user_socket_listener_old_2501(self) -> None:
        """Listen for user data websocket messages."""
        try:
            async with self.user_ws as stream:
                while self.is_running:
                    # Record heartbeat for account websocket
                    self._record_heartbeat('account_websocket')
                    msg = await stream.recv()
                    await self._process_user_socket_message(msg)
    
        except Exception as e:
            self.logger.error(f"Error in user socket listener: {str(e)}")
            await self.stop()
            raise

    async def _user_socket_listener(self) -> None:
        """Listen for user data websocket messages."""
        try:
            async with self.user_ws as stream:
                while self.is_running:
                    # Record heartbeat for account websocket
                    self._record_heartbeat('account_websocket')
                    msg = await stream.recv()
                    await self._process_user_socket_message(msg)
        except asyncio.CancelledError:
            self.logger.info("WebSocket listener task was cancelled")
        except Exception as e:
            self.logger.error(f"Error in user socket listener: {str(e)}")
        finally:
            self.logger.info("WebSocket listener stopped")
            self._ws_connected = False
            
    async def _process_user_socket_message(self, msg: Dict) -> None:
        """Process incoming websocket messages."""
        try:
            if msg['e'] == 'outboundAccountPosition':
                # Process balance update
                with self.balances_lock:
                    for balance in msg['B']:
                        asset = balance['a']
                        free = Decimal(balance['f'])
                        locked = Decimal(balance['l'])

                        self.balances[asset] = {
                            'free': free,
                            'locked': locked
                        }

                        # Notify balance callbacks
                        for callback in self._balance_callbacks:
                            try:
                                callback(asset, free, locked)
                            except Exception as cb_error:
                                self.logger.error(f"Error in balance callback: {str(cb_error)}")

            elif msg['e'] == 'executionReport':
                # Process order update
                await self._handle_order_update(msg)

            elif msg['e'] == 'trade':
                # Process trade update
                await self._handle_trade_update(msg)

        except Exception as e:
            self.logger.error(f"Error processing socket message: {str(e)}")

    async def _handle_order_update(self, msg: Dict) -> None:
        """Handle order update messages."""
        try:
            order_id = msg['i']
            symbol = msg['s']
            status = msg['X']

            with self.orders_lock:
                # Update order status
                self.orders[symbol][order_id] = {
                    'orderId': order_id,
                    'symbol': symbol,
                    'status': status,
                    'side': msg['S'],
                    'type': msg['o'],
                    'quantity': float(msg['q']),
                    'price': float(msg['p']),
                    'updateTime': pd.Timestamp(msg['E'], unit='ms'),
                    'executedQty': float(msg['z']),
                    'cumQuote': float(msg['Z']),
                    'lastPrice': float(msg['L']) if msg['L'] != '0' else None
                }

                # Store update history
                self.order_updates[order_id].append({
                    'time': pd.Timestamp(msg['E'], unit='ms'),
                    'status': status,
                    'executedQty': float(msg['z']),
                    'lastPrice': float(msg['L']) if msg['L'] != '0' else None
                })

            # Notify order callbacks
            for callback in self._order_callbacks:
                try:
                    callback(self.orders[symbol][order_id])
                except Exception as cb_error:
                    self.logger.error(f"Error in order callback: {str(cb_error)}")

        except Exception as e:
            self.logger.error(f"Error handling order update: {str(e)}")

    async def _handle_trade_update(self, msg: Dict) -> None:
        """Handle trade update messages."""
        try:
            trade = {
                'symbol': msg['s'],
                'id': msg['t'],
                'orderId': msg['i'],
                'side': msg['S'],
                'price': float(msg['p']),
                'quantity': float(msg['q']),
                'commission': float(msg['n']),
                'commissionAsset': msg['N'],
                'time': pd.Timestamp(msg['T'], unit='ms')
            }

            with self.trades_lock:
                self.trades[msg['s']].append(trade)

            # Notify trade callbacks
            for callback in self._trade_callbacks:
                try:
                    callback(trade)
                except Exception as cb_error:
                    self.logger.error(f"Error in trade callback: {str(cb_error)}")

        except Exception as e:
            self.logger.error(f"Error handling trade update: {str(e)}")

    def start_old(self):
        """Start real-time account monitoring."""
        if self.is_running:
            print("Account monitor is already running!")
            return

        self.is_running = True

        # Start websocket in event loop
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._initialize_user_socket())

    async def start(self):
        """Start real-time account monitoring with better error handling."""
        try:
            if self.is_running:
                self.logger.info("Account monitor is already running!")
                return
    
            self.is_running = True
            
            # Test API connection first
            try:
                account_info = self.client.get_account()
                self.logger.info("API connection test successful")
            except Exception as api_error:
                self.logger.error(f"API connection test failed: {str(api_error)}")
                raise
    
            # Initialize websocket connection
            try:
                await self._initialize_user_socket()
            except Exception as ws_error:
                self.logger.error(f"WebSocket initialization failed: {str(ws_error)}")
                self.is_running = False
                raise
    
            self.logger.info("Account monitoring started successfully")
            
        except Exception as e:
            self.is_running = False
            self.logger.error(f"Error starting account monitor: {str(e)}")
            raise
    
    async def stop_old(self):
        """Stop real-time account monitoring."""
        self.is_running = False

        # Stop websocket
        await self._cleanup()

        print("Account monitor stopped!")

    async def stop(self):
        """Stop real-time account monitoring."""
        try:
            self.is_running = False
            if self._ws_connected:
                await asyncio.wait_for(self._cleanup(), timeout=5.0)
            self.logger.info("Account monitor stopped!")
        except asyncio.TimeoutError:
            self.logger.error("Stop operation timed out")
        except Exception as e:
            self.logger.error(f"Error stopping account monitor: {e}")
        finally:
            self.is_running = False
    
    async def _cleanup_depreciated(self):
        """Clean up connections."""
        try:
            if self.socket_manager:
                await self.socket_manager.close()

            if self.async_client:
                await self.async_client.close_connection()

        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

    async def _cleanup_old(self):
        try:
            if self.socket_manager:
                await asyncio.wait_for(self.socket_manager.close(), timeout=5.0)
            if self.async_client:
                await asyncio.wait_for(self.async_client.close_connection(), timeout=5.0)
        except asyncio.TimeoutError:
            self.logger.error("Timeout during cleanup")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

    async def _cleanup_old_2(self):
        """Clean up connections with proper error handling."""
        try:
            # Stop user data stream if active
            if self.user_ws:
                try:
                    await self.user_ws.__aexit__(None, None, None)
                except Exception as e:
                    self.logger.error(f"Error closing user data stream: {e}")
    
            # Cancel any existing tasks
            tasks = [task for task in asyncio.all_tasks() 
                    if task is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
    
            # Close async client connection if it exists
            if self.async_client:
                try:
                    await self.async_client.close_connection()
                except Exception as e:
                    self.logger.error(f"Error closing async client: {e}")
    
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
        finally:
            # Reset connection-related attributes
            self._ws_connected = False
            self.socket_manager = None
            self.user_ws = None
            self.async_client = None

    async def _cleanup(self):
        """Clean up connections with proper cancellation handling."""
        try:
            # First stop the websocket connection
            self.is_running = False
    
            # Close user data stream if active
            if self.user_ws:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self.user_ws.__aexit__(None, None, None)),
                        timeout=2.0
                    )
                except Exception as e:
                    self.logger.error(f"Error closing user data stream: {e}")
    
            # Cancel all registered tasks
            for task in self._tasks:
                task.cancel()
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=1.0)
                except (asyncio.CancelledError, TimeoutError):
                    pass
    
            # Close async client
            if hasattr(self, 'async_client') and self.async_client:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self.async_client.close_connection()),
                        timeout=2.0
                    )
                except Exception as e:
                    self.logger.error(f"Error closing async client: {e}")
    
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")
        finally:
            # Reset connection attributes
            self._ws_connected = False
            self.socket_manager = None
            self.user_ws = None
            self.async_client = None
            self.logger.info("Cleanup completed")

    def get_balance(self, symbol: Optional[str] = None) -> Union[Dict, Decimal]:
        """Get current balance for all tracked assets or a specific symbol."""
        with self.balances_lock:
            if symbol:
                if self.tracked_currencies and symbol not in self.tracked_currencies:
                    return Decimal('0')  # Return 0 for untracked currencies
                return self.balances.get(symbol, {'free': Decimal('0')})['free']

            # Filter balances for tracked currencies
            if self.tracked_currencies:
                return {asset: data for asset, data in self.balances.items() if asset in self.tracked_currencies}
            return self.balances.copy()

    def get_formatted_balance(self, min_value: float = 0.0) -> pd.DataFrame:
        """Get formatted balance as a DataFrame, filtered by minimum value."""
        try:
            with self.balances_lock:
                # Create DataFrame from balances
                df = pd.DataFrame([
                    {
                        'Asset': asset,
                        'Free': float(data['free']),
                        'Locked': float(data['locked']),
                        'Total': float(data['free'] + data['locked'])
                    }
                    for asset, data in self.balances.items()
                ])
                
                if df.empty:
                    return pd.DataFrame(columns=['Asset', 'Free', 'Locked', 'Total'])
                
                # Filter by minimum value and sort
                df = df[df['Total'] > min_value].sort_values('Total', ascending=False)
                return df
                
        except Exception as e:
            self.logger.error(f"Error formatting balance: {str(e)}")
            return pd.DataFrame()

    def get_open_orders(self, symbol: Optional[str] = None) -> Union[Dict, List[Dict]]:
        """Get all open orders or open orders for a specific symbol."""
        try:
            with self.orders_lock:
                if symbol:
                    if self.tracked_currencies and symbol.replace('USDT', '') not in self.tracked_currencies:
                        return []  # Return empty list for untracked currencies
                    return [
                        order for order in self.orders[symbol].values()
                        if order['status'] == 'NEW'
                    ]

                open_orders = []
                for symbol, symbol_orders in self.orders.items():
                    base_currency = symbol.replace('USDT', '')  # Assuming USDT pairs
                    if self.tracked_currencies and base_currency not in self.tracked_currencies:
                        continue  # Skip untracked currencies

                    open_orders.extend([
                        order for order in symbol_orders.values()
                        if order['status'] == 'NEW'
                    ])
                return open_orders

        except Exception as e:
            self.logger.error(f"Error getting open orders: {str(e)}")
            return [] if symbol else {}

    def get_order_history_old(self, 
                         symbol: Optional[str] = None,
                         status: Optional[str] = None,
                         limit: int = 100) -> pd.DataFrame:
        """Get order history with optional filtering."""
        try:
            with self.orders_lock:
                orders_list = []
                
                for sym, sym_orders in self.orders.items():
                    if symbol and sym != symbol:
                        continue
                        
                    for order in sym_orders.values():
                        if status and order['status'] != status:
                            continue
                        orders_list.append(order)
                
                if not orders_list:
                    return pd.DataFrame()
                    
                df = pd.DataFrame(orders_list)
                df = df.sort_values('updateTime', ascending=False).head(limit)
                return df
                
        except Exception as e:
            self.logger.error(f"Error getting order history: {str(e)}")
            return pd.DataFrame()

    def get_trade_history(self,
                         symbol: Optional[str] = None,
                         start_time: Optional[datetime] = None,
                         limit: int = 500) -> pd.DataFrame:
        """
        Get trade history with optional filters.
        """
        try:
            trades = []
            if symbol:
                #print(f"Fetching trades for {symbol}...")
                try:
                    trades = self.client.get_my_trades(
                        symbol=symbol,
                        limit=limit
                    )
                except Exception as e:
                    print(f"Error fetching trades for {symbol}: {e}")
            else:
                #print("Fetching trades for all tracked symbols...")
                for base_currency in self.tracked_currencies:
                    if base_currency != 'USDT':
                        symbol = f"{base_currency}USDT"
                        #print(f"Trying {symbol}...")
                        try:
                            symbol_trades = self.client.get_my_trades(
                                symbol=symbol,
                                limit=limit
                            )
                            #print(f"Found {len(symbol_trades)} trades for {symbol}")
                            trades.extend(symbol_trades)
                        except Exception as e:
                            print(f"Error fetching trades for {symbol}: {e}")
    
            #print(f"Total trades found: {len(trades)}")
            
            if not trades:
                print("No trades found")
                return pd.DataFrame()
    
            # Create DataFrame and format
            df = pd.DataFrame(trades)
            
            # Convert timestamps
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'], unit='ms')
    
            # Format numeric columns
            numeric_columns = ['price', 'qty', 'quoteQty', 'commission']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
    
            # Select and reorder important columns
            columns = [
                'symbol', 'id', 'orderId', 'side', 
                'price', 'qty', 'quoteQty', 
                'commission', 'commissionAsset',
                'time'
            ]
            df = df[[col for col in columns if col in df.columns]]
    
            # Sort by time
            df = df.sort_values('time', ascending=False)
    
            return df
    
        except Exception as e:
            print(f"Error getting trade history: {e}")
            return pd.DataFrame()
    
    def get_order_history(self, 
                         symbol: Optional[str] = None,
                         status: Optional[str] = None,
                         start_time: Optional[datetime] = None,
                         limit: int = 100) -> pd.DataFrame:
        """
        Get order history with optional filtering.
        """
        try:
            # First verify account connection...
            print("Checking account connection...")
            account_info = self.client.get_account()
            print(f"Account status: {account_info.get('canTrade', False)}")
    
            # Fetch orders as before...
            orders = []
            if symbol:
                print(f"Fetching orders for {symbol}...")
                try:
                    orders = self.client.get_all_orders(
                        symbol=symbol,
                        limit=limit
                    )
                except Exception as e:
                    print(f"Error fetching orders for {symbol}: {e}")
            else:
                print("Fetching orders for all tracked symbols...")
                for base_currency in self.tracked_currencies:
                    if base_currency != 'USDT':
                        symbol = f"{base_currency}USDT"
                        print(f"Trying {symbol}...")
                        try:
                            symbol_orders = self.client.get_all_orders(
                                symbol=symbol,
                                limit=limit
                            )
                            print(f"Found {len(symbol_orders)} orders for {symbol}")
                            orders.extend(symbol_orders)
                        except Exception as e:
                            print(f"Error fetching orders for {symbol}: {e}")
    
            print(f"Total orders found: {len(orders)}")
            
            if not orders:
                print("No orders found")
                return pd.DataFrame()
    
            # Create DataFrame and format
            df = pd.DataFrame(orders)
            
            # Convert timestamps
            time_columns = ['time', 'updateTime', 'workingTime']
            for col in time_columns:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], unit='ms')
    
            # Format numeric columns
            numeric_columns = ['price', 'origQty', 'executedQty', 'cummulativeQuoteQty']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
    
            # Select and reorder important columns
            columns = [
                'symbol', 'orderId', 'type', 'side', 'status',
                'price', 'origQty', 'executedQty', 'cummulativeQuoteQty',
                'time', 'updateTime'
            ]
            df = df[columns]
    
            # Sort by time
            df = df.sort_values('time', ascending=False)
    
            return df
    
        except Exception as e:
            print(f"Error getting order history: {e}")
            return pd.DataFrame()

    def get_order_updates(self, order_id: str) -> List[Dict]:
        """Get update history for a specific order."""
        with self.orders_lock:
            return self.order_updates[order_id].copy()

    def add_balance_callback(self, callback: Callable[[str, Decimal, Decimal], None]) -> None:
        """Add callback for balance updates."""
        self._balance_callbacks.append(callback)

    def add_order_callback(self, callback: Callable[[Dict], None]) -> None:
        """Add callback for order updates."""
        self._order_callbacks.append(callback)

    def add_trade_callback(self, callback: Callable[[Dict], None]) -> None:
        """Add callback for trade updates."""
        self._trade_callbacks.append(callback)
        
    def get_tracked_symbols(self) -> List[str]:
        """Get list of symbols with non-zero balances or open orders."""
        tracked = set()
        
        # Add symbols with balances
        for asset in self.balances:
            if asset != 'USDT' and (self.balances[asset]['free'] > 0 or self.balances[asset]['locked'] > 0):
                tracked.add(f"{asset}USDT")  # Assuming USDT pairs
                
        return list(tracked)

    def get_daily_pnl(self, days: int = 30) -> pd.DataFrame:
        """Get daily profit/loss analysis."""
        try:
            # Get recent trades
            start_time = datetime.now() - timedelta(days=days)
            trades = self.get_trade_history(start_time=start_time)
            
            if trades.empty:
                return pd.DataFrame()
                
            # Calculate daily PnL
            trades['date'] = trades['time'].dt.date
            daily_pnl = trades.groupby('date').agg({
                'price': 'mean',
                'qty': 'sum',
                'quoteQty': 'sum',
                'commission': 'sum'
            })
            
            # Calculate net PnL (assuming USDT commission)
            daily_pnl['net_pnl'] = daily_pnl['quoteQty'] - daily_pnl['commission']
            
            return daily_pnl
            
        except Exception as e:
            self.logger.error(f"Error calculating daily PnL: {str(e)}")
            return pd.DataFrame()

    def get_risk_metrics(self) -> Dict[str, float]:
        """Get current risk metrics."""
        try:
            metrics = {
                'total_balance_usdt': self._calculate_total_balance_usdt(),
                'largest_position_pct': self._calculate_largest_position_pct(),
                'daily_returns_std': self._calculate_volatility(),
                'sharpe_ratio': self._calculate_sharpe_ratio(),
                'max_drawdown': self._calculate_max_drawdown()
            }
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error calculating risk metrics: {str(e)}")
            return {}

    def _get_position_size(self, asset: str, balance: Dict[str, Decimal]) -> float:
        """
        Calculate the position size for a given asset in USDT.
    
        Args:
            asset (str): The asset symbol (e.g., 'BTC', 'ETH').
            balance (Dict[str, Decimal]): The balance dictionary containing 'free' and 'locked' amounts.
    
        Returns:
            float: The total position size in USDT.
        """
        if asset == 'USDT':
            return float(balance['free'] + balance['locked'])
        
        try:
            # Fetch the current price for the asset in USDT
            ticker = self.client.get_ticker(symbol=f"{asset}USDT")
            price = float(ticker['lastPrice'])
            return float(balance['free'] + balance['locked']) * price
        except Exception as e:
            self.logger.error(f"Error fetching price for {asset}: {str(e)}")
            return 0.0

    def _calculate_total_balance_usdt(self) -> float:
        """
        Calculate the total balance in USDT.
    
        Returns:
            float: The total balance in USDT.
        """
        try:
            total = Decimal('0')
            
            for asset, balance in self.balances.items():
                position_size = self._get_position_size(asset, balance)
                total += Decimal(str(position_size))
                
            return float(total)
            
        except Exception as e:
            self.logger.error(f"Error calculating total balance: {str(e)}")
            return 0.0
            
    def _calculate_largest_position_pct(self) -> float:
        """
        Calculate the largest position as a percentage of the total portfolio.
    
        Returns:
            float: The percentage of the largest position.
        """
        try:
            total = self._calculate_total_balance_usdt()
            if total == 0:
                return 0.0
                
            largest = 0.0
            for asset, balance in self.balances.items():
                position_size = self._get_position_size(asset, balance)
                largest = max(largest, position_size)
                
            return (largest / total) * 100
                
        except Exception as e:
            self.logger.error(f"Error calculating largest position: {str(e)}")
            return 0.0
        
    def _calculate_total_balance_usdt_depreciated(self) -> float:
        """Calculate total balance in USDT."""
        try:
            total = Decimal('0')
            
            for asset, balance in self.balances.items():
                if asset == 'USDT':
                    total += balance['free'] + balance['locked']
                else:
                    # Get current price for asset
                    try:
                        ticker = self.client.get_ticker(symbol=f"{asset}USDT")
                        price = Decimal(ticker['lastPrice'])
                        total += (balance['free'] + balance['locked']) * price
                    except:
                        continue
                        
            return float(total)
            
        except Exception as e:
            self.logger.error(f"Error calculating total balance: {str(e)}")
            return 0.0

    def _calculate_largest_position_pct_depreciated(self) -> float:
        """Calculate largest position as percentage of portfolio."""
        try:
            total = self._calculate_total_balance_usdt()
            if total == 0:
                return 0.0
                
            largest = 0.0
            for asset, balance in self.balances.items():
                if asset == 'USDT':
                    position_size = float(balance['free'] + balance['locked'])
                else:
                    try:
                        ticker = self.client.get_ticker(symbol=f"{asset}USDT")
                        price = float(ticker['lastPrice'])
                        position_size = float(balance['free'] + balance['locked']) * price
                    except:
                        continue
                        
                largest = max(largest, position_size)
                
            return (largest / total) * 100
            
        except Exception as e:
            self.logger.error(f"Error calculating largest position: {str(e)}")
            return 0.0

    def _calculate_volatility(self, days: int = 30) -> float:
        """Calculate portfolio daily return volatility."""
        try:
            daily_pnl = self.get_daily_pnl(days)
            if daily_pnl.empty:
                return 0.0
                
            daily_returns = daily_pnl['net_pnl'].pct_change()
            return float(daily_returns.std() * np.sqrt(252))  # Annualized
            
        except Exception as e:
            self.logger.error(f"Error calculating volatility: {str(e)}")
            return 0.0

    def _calculate_sharpe_ratio(self, risk_free_rate: float = 0.02) -> float:
        """Calculate Sharpe ratio."""
        try:
            daily_pnl = self.get_daily_pnl()
            if daily_pnl.empty:
                return 0.0
                
            daily_returns = daily_pnl['net_pnl'].pct_change()
            excess_returns = daily_returns - (risk_free_rate / 252)
            
            if excess_returns.std() == 0:
                return 0.0
                
            return float(np.sqrt(252) * excess_returns.mean() / excess_returns.std())
            
        except Exception as e:
            self.logger.error(f"Error calculating Sharpe ratio: {str(e)}")
            return 0.0

    def _calculate_max_drawdown(self) -> float:
        """Calculate maximum drawdown percentage."""
        try:
            daily_pnl = self.get_daily_pnl()
            if daily_pnl.empty:
                return 0.0
                
            cumulative = daily_pnl['net_pnl'].cumsum()
            rolling_max = cumulative.expanding().max()
            drawdowns = (cumulative - rolling_max) / rolling_max
            
            return float(abs(drawdowns.min()) * 100)
            
        except Exception as e:
            self.logger.error(f"Error calculating max drawdown: {str(e)}")
            return 0.0

    def format_number_to_binance_precision(self, symbol: str, number: float, is_price: bool = True) -> str:
        """
        Format numbers according to Binance's precision requirements for a specific symbol.
    
        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
            number: The number to format
            is_price: True if formatting price, False if formatting quantity
    
        Returns:
            str: Formatted number string according to Binance requirements
    
        Example:
            >>> account.format_number_to_binance_precision('BTCUSDT', 29123.4567891, is_price=True)
            '29123.46'  # BTC price precision is 2 decimals
            >>> account.format_number_to_binance_precision('BTCUSDT', 0.12345678, is_price=False)
            '0.12345'   # BTC quantity precision is 5 decimals
        """
        try:
            # Get symbol info from Binance
            symbol_info = self.client.get_symbol_info(symbol)
    
            if not symbol_info:
                raise ValueError(f"Symbol {symbol} not found")
    
            if is_price:
                # Get price precision from price filter
                price_filter = next(filter(lambda x: x['filterType'] == 'PRICE_FILTER',
                                         symbol_info['filters']))
                precision = str(price_filter['tickSize'])[::-1].find('.')
    
                # Format price
                return f"{float(number):.{precision}f}"
            else:
                # Get quantity precision from lot size filter
                lot_size_filter = next(filter(lambda x: x['filterType'] == 'LOT_SIZE',
                                            symbol_info['filters']))
                precision = str(lot_size_filter['stepSize'])[::-1].find('.')
    
                # Format quantity
                return f"{float(number):.{precision}f}"
    
        except Exception as e:
            self.logger.error(f"Error formatting number for {symbol}: {str(e)}")
            raise

    def get_position_value(self, symbol: str) -> Dict[str, float]:
        """
        Get the current value of a position in USDT.
    
        Args:
            symbol: Trading pair (e.g., 'BTCUSDT')
    
        Returns:
            Dict containing position details:
                - free_value: Value of free balance
                - locked_value: Value of locked balance
                - total_value: Total position value
                - current_price: Current price
                - quantity: Total quantity held
    
        Example:
            >>> account.get_position_value('BTCUSDT')
            {
                'free_value': 15000.0,
                'locked_value': 5000.0,
                'total_value': 20000.0,
                'current_price': 40000.0,
                'quantity': 0.5
            }
        """
        try:
            # Extract base asset (e.g., 'BTC' from 'BTCUSDT')
            base_asset = symbol.replace('USDT', '')
    
            with self.balances_lock:
                # Get balance
                balance = self.balances.get(base_asset, {'free': Decimal('0'), 'locked': Decimal('0')})
                free_balance = float(balance['free'])
                locked_balance = float(balance['locked'])
                total_balance = free_balance + locked_balance
    
                # Get current price
                ticker = self.client.get_ticker(symbol=symbol)
                current_price = float(ticker['lastPrice'])
    
                return {
                    'free_value': free_balance * current_price,
                    'locked_value': locked_balance * current_price,
                    'total_value': total_balance * current_price,
                    'current_price': current_price,
                    'quantity': total_balance
                }
    
        except Exception as e:
            self.logger.error(f"Error getting position value for {symbol}: {str(e)}")
            return {
                'free_value': 0.0,
                'locked_value': 0.0,
                'total_value': 0.0,
                'current_price': 0.0,
                'quantity': 0.0
            }

    def get_position_exposure(self) -> pd.DataFrame:
        """
        Calculate exposure of all positions relative to total portfolio value.
    
        Returns:
            DataFrame with columns:
                - symbol: Trading pair
                - value_usdt: Position value in USDT
                - exposure_pct: Percentage of total portfolio
                - quantity: Amount of asset
                - current_price: Current price
    
        Example:
            >>> account.get_position_exposure()
               symbol  value_usdt  exposure_pct  quantity  current_price
            0  BTCUSDT   20000.0         40.0      0.5       40000.0
            1  ETHUSDT   15000.0         30.0      5.0        3000.0
            2  BNBUSDT   10000.0         20.0     25.0         400.0
            3  USDTUSDT   5000.0         10.0   5000.0           1.0
        """
        try:
            # Get total portfolio value
            total_value = self._calculate_total_balance_usdt()
    
            if total_value == 0:
                return pd.DataFrame(columns=['symbol', 'value_usdt', 'exposure_pct',
                                          'quantity', 'current_price'])
    
            positions = []
            # Get all tracked symbols
            symbols = self.get_tracked_symbols()
    
            for symbol in symbols:
                position = self.get_position_value(symbol)
    
                positions.append({
                    'symbol': symbol,
                    'value_usdt': position['total_value'],
                    'exposure_pct': (position['total_value'] / total_value) * 100,
                    'quantity': position['quantity'],
                    'current_price': position['current_price']
                })
    
            # Create DataFrame and sort by exposure
            df = pd.DataFrame(positions)
            if not df.empty:
                df = df.sort_values('exposure_pct', ascending=False)
    
            return df
    
        except Exception as e:
            self.logger.error(f"Error calculating position exposure: {str(e)}")
            return pd.DataFrame()
