import vectorbtpro as vbt
import pandas as pd
from datetime import datetime
import logging

class EMACrossStrategy:
    def __init__(self):
        """Initialize the EMA Crossover Strategy."""
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Initialize system monitor
        self.monitor = None
        
        self.data_ = None
        self._h1_sma = None
        self._h4_sma = None
        self._d1_sma = None
        self._entries_long = None
        self._exits_long = None
        self._entries_short = None
        self._exits_short = None
        self._last_update = None

    def update(self, data_vbt: vbt.Data) -> bool:
        """Update strategy with new data and calculate signals."""
        try:
            if data_vbt is None:
                self.logger.error("No data provided for strategy update")
                return False

            self.data_ = data_vbt
            self._calculate_signals()
            self._last_update = datetime.now()
            return True

        except Exception as e:
            self.logger.error(f"Error updating strategy: {e}")
            return False

    def _calculate_signals(self):
        """Calculate EMAs and entry/exit signals."""
        try:
            # Optional heartbeat recording if monitor exists
            if self.monitor is not None:
                self.monitor.record_heartbeat('strategy')

            if self.data_ is None:
                return

            # Calculate EMAs for different timeframes - keep your original working code
            mas = vbt.talib("EMA").run(
                self.data_.get("Close"), 
                skipna=True,
                timeframe=["1h", "4h", "1d"],
                broadcast_kwargs=dict(wrapper_kwargs=dict(freq="1h"))
            )

            # Store EMAs
            self._h1_sma = mas.ema['1h']
            self._h4_sma = mas.ema['4h']
            self._d1_sma = mas.ema['1d']

            # Calculate entry/exit signals
            self._entries_long = self.data_.get('Close').vbt.crossed_above(self._h4_sma) & (self._h4_sma < self._d1_sma)
            self._exits_long = self._h4_sma.vbt.crossed_below(self._d1_sma)
            self._entries_short = self.data_.get('Close').vbt.crossed_below(self._h4_sma) & (self._h4_sma > self._d1_sma)
            self._exits_short = self._h4_sma.vbt.crossed_above(self._d1_sma)

        except Exception as e:
            self.logger.error(f"Error calculating signals: {e}")
            self._reset_signals()

    def _reset_signals(self):
        """Reset all signals to None in case of calculation error."""
        self._h1_sma = None
        self._h4_sma = None
        self._d1_sma = None
        self._entries_long = None
        self._exits_long = None
        self._entries_short = None
        self._exits_short = None

    def get_all_signals(self):
        """Get all current signals and indicators."""
        return {
            'data_': self.data_,
            'ema_1h': self._h1_sma,
            'ema_4h': self._h4_sma,
            'ema_1d': self._d1_sma,
            'entries_long': self._entries_long,
            'exits_long': self._exits_long,
            'entries_short': self._entries_short,
            'exits_short': self._exits_short,
            'last_update': self._last_update
        }
