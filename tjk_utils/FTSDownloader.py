from datetime import date, datetime, timedelta
import vectorbtpro as vbt
import pandas as pd
import os
import pytz
import re
import logging
import asyncio
import concurrent.futures
from functools import partial
import nest_asyncio
nest_asyncio.apply()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FTSDownloader:
    def __init__(self, hdf_file_path):
        """
        Initialize the FTSDownloader with a path to the HDF file.

        :param hdf_file_path: Path to the HDF5 file for data storage
        """
        self.hdf_file_path = hdf_file_path
        # Create directory if it doesn't exist
        if os.path.dirname(hdf_file_path):
            os.makedirs(os.path.dirname(hdf_file_path), exist_ok=True)

    def _ensure_tz_aware(self, data, target_tz='UTC'):
        """
        Helper method to ensure DataFrame has proper timezone information.

        :param data: DataFrame with datetime index
        :param target_tz: Target timezone to convert to
        :return: DataFrame with proper timezone
        """
        if data is None:
            return None

        data = data.copy()
        if data.index.tz is None:
            data.index = data.index.tz_localize('UTC')
        if target_tz != 'UTC':
            data = data.tz_convert(target_tz)
        return data

    def _validate_columns(self, data):
        """
        Helper method to validate required columns.

        :param data: DataFrame to validate
        :return: DataFrame with only required columns
        :raises ValueError: If required columns are missing
        """
        required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing = [col for col in required_columns if col not in data.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")
        return data[required_columns].copy()

    def _convert_dates_to_utc(self, start_date=None, end_date=None):
        """
        Helper method to convert dates to UTC timezone.

        :param start_date: Start date string or timestamp
        :param end_date: End date string or timestamp
        :return: Tuple of UTC timestamps
        """
        if start_date is not None:
            start = pd.Timestamp(start_date)
            start = start.tz_localize('UTC') if start.tz is None else start.tz_convert('UTC')
        else:
            start = None

        if end_date is not None:
            end = pd.Timestamp(end_date)
            end = end.tz_localize('UTC') if end.tz is None else end.tz_convert('UTC')
        else:
            end = None

        return start, end
    
    def sanitize_symbol(self, symbol):
        """
        Sanitize the symbol name to be a valid Python identifier.

        :param symbol: The original symbol name
        :return: A sanitized symbol name
        """
        return re.sub(r'\W+', '_', symbol)

    def get_key(self, symbol, periodicity, loader, exchange=None):
        """
        Generate a unique key for storing data in HDF5 file.

        :param symbol: The financial instrument symbol
        :param periodicity: Data periodicity (e.g., '1d', '1h', '1m')
        :param loader: Data loader to use ('tradingview', 'ccxt')
        :param exchange: Exchange name (optional)
        :return: A unique key for the HDF5 store
        """
        sanitized_symbol = self.sanitize_symbol(symbol)
        exchange_part = f"_{exchange}" if exchange else ""
        return f"{sanitized_symbol}_{periodicity}_{loader}{exchange_part}"

    def has_data(self, symbol, periodicity, loader, exchange=None):
        """
        Check if data exists in the store for given parameters.

        :param symbol: The financial instrument symbol
        :param periodicity: Data periodicity
        :param loader: Data loader used
        :param exchange: Exchange name (optional)
        :return: Boolean indicating if data exists
        """
        try:
            key = self.get_key(symbol, periodicity, loader, exchange)
            with pd.HDFStore(self.hdf_file_path) as store:
                exists = key in store
            logger.debug(f"Checking data existence for key {key}: {'exists' if exists else 'does not exist'}")
            return exists
        except Exception as e:
            logger.error(f"Error checking data existence: {e}")
            return False

    def download_data(self, symbol, start_date=(date.today() - timedelta(days=6000)).strftime("%Y-%m-%d"), 
                     end_date=date.today().strftime("%Y-%m-%d"), loader='tradingview', 
                     timezone='Europe/Berlin', periodicity='1m', exchange=None):
        """
        Download financial time series data.

        :param symbol: The financial instrument symbol
        :param start_date: Start date in 'YYYY-MM-DD' format
        :param end_date: End date in 'YYYY-MM-DD' format
        :param loader: Data loader to use ('tradingview', 'ccxt')
        :param timezone: Timezone for the data
        :param periodicity: Data periodicity
        :param exchange: Exchange name (optional)
        :return: DataFrame with downloaded data
        """
        try:
            logger.info(f"Downloading data for {symbol} using {loader}")
            if loader == 'tradingview':
                data = self._download_tradingview(symbol, periodicity, timezone, exchange)
            elif loader == 'ccxt':
                data = self._download_ccxt(symbol, start_date, end_date, periodicity, exchange)
            else:
                raise ValueError(f"Unsupported loader: {loader}")

            if data is not None and timezone != 'UTC':
                data = data.tz_convert(timezone)

            if data is not None:
                logger.info(f"Successfully downloaded data for {symbol}. Shape: {data.shape}")
            return data

        except Exception as e:
            logger.error(f"Error downloading data for {symbol}: {e}")
            return None

    def _download_tradingview(self, symbol, periodicity, timezone, exchange):
        """Internal method for downloading data from TradingView."""
        try:
            data = vbt.TVData.pull(symbol, 
                                timeframe=periodicity,
                                tz=timezone,
                                exchange=exchange).get()
            return data
        except Exception as e:
            logger.error(f"Error downloading TradingView data for {symbol}: {e}")
            return None

    def _download_ccxt(self, symbol, start_date, end_date, periodicity, exchange):
        """Internal method for downloading data from CCXT."""
        try:
            # Convert dates to UTC timestamps
            start, end = self._convert_dates_to_utc(start_date, end_date)

            if end is None:
                # Set end date to midnight of next day to ensure we get all data
                end = (pd.Timestamp.now(tz='UTC') + pd.Timedelta(days=1)).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )

            logger.info(f"Downloading CCXT data for {symbol} from {start} to {end}")

            data = vbt.CCXTData.pull(
                symbol,
                timeframe=periodicity,
                start=start,
                end=end,
                exchange=exchange,
                silence_warnings=False
            ).get()

            if data is None or data.empty:
                logger.warning(f"CCXT returned empty data for {symbol}")
                return None

            # Validate data structure
            try:
                data = self._validate_columns(data)
            except ValueError as e:
                logger.error(f"Invalid data structure: {e}")
                return None

            logger.info(f"Successfully downloaded CCXT data for {symbol}. Shape: {data.shape}")
            return data

        except Exception as e:
            logger.error(f"Error downloading CCXT data for {symbol}: {e}")
            logger.error(f"Download parameters: symbol={symbol}, exchange={exchange}, timeframe={periodicity}")
            return None

    def load_data(self, symbol, periodicity, loader='tradingview', exchange=None,
                 timezone='Europe/Berlin', start_date=None, end_date=None, auto_update=True):
        """
        Load persisted data from the HDF5 file.
        """
        try:
            key = self.get_key(symbol, periodicity, loader, exchange)
            logger.info(f"Loading data for key: {key}")

            if not os.path.exists(self.hdf_file_path) or not self.has_data(symbol, periodicity, loader, exchange):
                if auto_update:
                    logger.info(f"No data found for {symbol}. Attempting automatic update...")
                    data = self.update_data(symbol, start_date, end_date, loader, timezone, periodicity, exchange)
                    if data is None:
                        return None
                else:
                    logger.warning(f"No data available for {symbol} and auto-update is disabled")
                    return None

            with pd.HDFStore(self.hdf_file_path) as store:
                data = store[key]

            # Convert timezone and handle date filtering
            data = self._ensure_tz_aware(data, timezone)

            # Handle date filtering
            start, end = self._convert_dates_to_utc(start_date, end_date)
            if start is not None:
                data = data[data.index >= start]
            if end is not None:
                data = data[data.index <= end]

            return vbt.Data.from_data(data)

        except Exception as e:
            logger.error(f"Error loading data for {symbol}: {e}")
            return None
    
    def persist_data(self, symbol, data, periodicity, loader, exchange=None):
        """
        Persist the downloaded data to HDF5 file.
        """
        try:
            key = self.get_key(symbol, periodicity, loader, exchange)
            logger.info(f"Persisting data for key: {key}")

            # Ensure data has timezone information and convert to UTC
            data = self._ensure_tz_aware(data, 'UTC')

            # Validate and select required columns
            data_to_persist = self._validate_columns(data)

            with pd.HDFStore(self.hdf_file_path) as store:
                store.put(key, data_to_persist, format='table', data_columns=True)
                logger.info(f"Successfully persisted data for {symbol}")

        except Exception as e:
            logger.error(f"Error persisting data for {symbol}: {e}")
            raise
    

    def update_data(self, symbol, start_date=None, end_date=None, loader='tradingview',
                    timezone='Europe/Berlin', periodicity='1m', exchange=None):
        """
        Updated update_data method with improved TradingView handling.
        """
        try:
            logger.info(f"Updating data for {symbol}")
            key = self.get_key(symbol, periodicity, loader, exchange)
    
            if loader == 'tradingview':
                # For TradingView, we need special handling
                last_timestamp = None
                if self.has_data(symbol, periodicity, loader, exchange):
                    last_timestamp = self.get_last_timestamp(key)
                    logger.info(f"Found existing data with last timestamp: {last_timestamp}")
    
                # Download complete dataset from TradingView
                new_data = self.download_data(symbol, None, end_date, loader, 'UTC', periodicity, exchange)
                if new_data is None or new_data.empty:
                    logger.warning(f"No data downloaded for {symbol} from TradingView")
                    return None
    
                # Ensure timezone info and handle the data
                new_data = self._ensure_tz_aware(new_data, 'UTC')
                
                if last_timestamp is not None:
                    # Calculate the number of new records that will be added
                    new_records = len(new_data[new_data.index > last_timestamp])
                    logger.info(f"Found {new_records} new records to append")
                
                self._append_data(key, new_data, loader='tradingview')
                return True
    
            else:  # Handle other data sources (like CCXT)
                if self.has_data(symbol, periodicity, loader, exchange):
                    last_timestamp = self.get_last_timestamp(key)
                    if last_timestamp is not None:
                        start_date = (last_timestamp + pd.Timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")
                        logger.info(f"Found existing data. Starting update from {start_date}")
    
                if start_date is None:
                    start_date = (date.today() - timedelta(days=6000)).strftime("%Y-%m-%d")
                if end_date is None:
                    end_date = (pd.Timestamp.now() + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    
                if pd.Timestamp(start_date) >= pd.Timestamp(end_date):
                    logger.info(f"No update needed for {symbol}")
                    return None
    
                new_data = self.download_data(symbol, start_date, end_date, loader, 'UTC', periodicity, exchange)
                if new_data is None or new_data.empty:
                    return None
    
                new_data = self._ensure_tz_aware(new_data, 'UTC')
                self._append_data(key, new_data, loader=loader)
                return True
    
        except Exception as e:
            logger.error(f"Error updating data for {symbol}: {e}")
            return None
    
    def update_all(self, start_date=None, end_date=None, 
                   loader='tradingview', timezone='Europe/Berlin'):
        """
        Run parallel updates using asyncio, compatible with Jupyter.
        """
        loop = asyncio.get_event_loop()
        if not loop.is_running():
            loop.run_until_complete(self.update_all_async(start_date, end_date, loader, timezone))
        else:
            # We're in Jupyter, use nest_asyncio
            loop.create_task(self.update_all_async(start_date, end_date, loader, timezone))
            
    async def update_all_async(self, start_date=None, end_date=None, 
                              loader='tradingview', timezone='Europe/Berlin'):
        """
        Asynchronously update all symbols in parallel.
        """
        try:
            with pd.HDFStore(self.hdf_file_path) as store:
                keys = store.keys()
    
            # Create a semaphore to limit concurrent operations
            semaphore = asyncio.Semaphore(5)  # Adjust the number based on your system
            
            async def update_with_semaphore(key):
                async with semaphore:
                    return await self._update_symbol_async(key, start_date, end_date, timezone)
    
            # Create tasks for all updates
            tasks = [
                update_with_semaphore(key)
                for key in keys
            ]
    
            # Run updates in parallel with progress tracking
            completed = 0
            total = len(tasks)
            
            for task in asyncio.as_completed(tasks):
                await task
                completed += 1
                print(f"Progress: {completed}/{total} symbols updated", end='\r')
                
            print("\nUpdate completed!")
    
        except Exception as e:
            logger.error(f"Error in update_all_async: {e}")
            raise
    
    async def _update_symbol_async(self, key, start_date, end_date, timezone):
        """
        Asynchronously update a single symbol with better error handling.
        """
        try:
            parts = key.strip('/').split('_')
            if len(parts) < 3:
                return
    
            symbol = parts[0]
            periodicity = parts[1]
            loader = parts[2]
            exchange = parts[3] if len(parts) > 3 else None
    
            # Use ThreadPoolExecutor for CPU-bound operations
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = self.update_data(symbol, start_date, end_date, loader, 
                                        timezone, periodicity, exchange)
                return await asyncio.get_event_loop().run_in_executor(pool, lambda: future)
    
        except Exception as e:
            logger.error(f"Error updating {key}: {e}")
            return None
    
    def _append_data(self, key, new_data, loader='tradingview'):
        """
        Efficiently append new data to existing HDF5 store with improved handling for TradingView data.
        """
        try:
            with pd.HDFStore(self.hdf_file_path) as store:
                if key in store:
                    # Get the last timestamp from existing data without loading full dataset
                    last_timestamp = self.get_last_timestamp(key)
                    
                    if last_timestamp is not None:
                        # Filter new data to only include records after the last timestamp
                        new_data = new_data[new_data.index > last_timestamp]
                        
                        if not new_data.empty:
                            logger.info(f"Appending {len(new_data)} new records for key: {key}")
                            # Use table format for efficient appending
                            store.append(key, new_data, format='table', data_columns=True)
                        else:
                            logger.info(f"No new data to append for {key}")
                    else:
                        logger.warning(f"No existing data found for {key}, creating new dataset")
                        store.put(key, new_data, format='table', data_columns=True)
                else:
                    logger.info(f"Creating new dataset for {key} with {len(new_data)} records")
                    store.put(key, new_data, format='table', data_columns=True)
                
                # Verify the append operation
                total_rows = store.get_storer(key).nrows
                logger.info(f"Total rows after append for {key}: {total_rows}")
                
        except Exception as e:
            logger.error(f"Error in _append_data for {key}: {e}")
            raise
    
    # Optimize HDF5 store configuration
    def _optimize_store(self):
        """
        Optimize HDF5 store settings for better performance.
        """
        with pd.HDFStore(self.hdf_file_path) as store:
            for key in store.keys():
                # Rewrite the data with optimized settings
                data = store[key]
                store.put(key, data, format='table', 
                         data_columns=True,
                         index=True,
                         track_times=False,  # Disable timestamp tracking
                         complevel=1,  # Light compression for better speed
                         complib='blosc:lz4')  # Fast compression algorithm

    def get_last_timestamp(self, key):
        """
        Efficiently get the last timestamp for a given key without loading the entire dataset.
        """
        try:
            with pd.HDFStore(self.hdf_file_path) as store:
                # Get only the last row's index using select
                last_row = store.select(key, start=-1)
                if not last_row.empty:
                    return last_row.index[-1]
        except Exception as e:
            logger.error(f"Error getting last timestamp for {key}: {e}")
        return None
    
    def info(self):
        """
        List detailed information about all persisted data using efficient HDF5 queries.
        """
        try:
            info_data = []
            with pd.HDFStore(self.hdf_file_path) as store:
                for key in store.keys():
                    try:
                        parts = key.strip('/').split('_')
                        
                        # Get first and last rows only
                        first_row = store.select(key, start=0, stop=1)
                        last_row = store.select(key, start=-1)
                        
                        # Get total number of rows without loading data
                        nrows = store.get_storer(key).nrows
                        
                        if not first_row.empty and not last_row.empty:
                            info_data.append({
                                'symbol': parts[0],
                                'periodicity': parts[1],
                                'loader': parts[2],
                                'exchange': parts[3] if len(parts) > 3 else None,
                                'start_date': first_row.index[0],
                                'end_date': last_row.index[-1],
                                'rows': nrows,
                                'timezone': str(first_row.index.tz)
                            })
                    except Exception as e:
                        logger.error(f"Error processing key {key}: {e}")
                        continue
    
            return pd.DataFrame(info_data)
    
        except Exception as e:
            logger.error(f"Error getting info: {e}")
            return pd.DataFrame()
    

# Example usage:
#downloader = FTSDownloader('data/market_data.h5')
#downloader.update_data('US100', '2022-12-01', periodicity='1d', exchange='CAPITALCOM')
#downloader.update_data('US100', periodicity='1m', exchange='CAPITALCOM')
#downloader.update_data('BTCUSDT', loader = "ccxt", periodicity='1m', exchange='binance')
#downloader.update_all('2022-12-01', '2022-12-31', loader='tradingview')
# downloader.delete_data('BTCUSDT', '1m', 'ccxt', 'binance')
#downloader.load_data('BTCUSDT', loader="ccxt", periodicity='1m', exchange='binance', start_date='2025-01-01').get()
#downloader.update_data('BTCUSDT', loader = "ccxt", periodicity='1m', exchange='binance')
#downloader.update_all()

# Download and store data
#downloader.update_data('US100', periodicity='1d', exchange='CAPITALCOM')
#downloader.update_data('US100', periodicity='1m', exchange='CAPITALCOM')
#downloader.update_data('BTCUSDT', loader="ccxt", periodicity='1d', exchange='binance')
#downloader.update_data('BTCUSDT', loader="ccxt", periodicity='1m', exchange='binance')
# Get info about stored data
#info_df = downloader.info()
#info_df
