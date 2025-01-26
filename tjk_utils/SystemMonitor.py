import asyncio
import logging
from datetime import datetime, timedelta
import os
from typing import Dict

class SystemMonitor:
    def __init__(self, log_dir: str = '../logs'):
        """
        Initialize system monitor.
        
        Args:
            log_dir: Directory for log files
        """
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        
        # Set up file handler for detailed logging
        self.setup_logging()
        
        # System state tracking
        self.last_heartbeat = {}
        self.component_status = {}
        self._is_running = False
        
        # Monitoring settings
        self.heartbeat_interval = 60  # seconds
        self.max_silence = 300  # 5 minutes max without updates
        
    def setup_logging(self):
        """Setup detailed logging configuration."""
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        
        # Create file handler with detailed formatting
        log_file = os.path.join(self.log_dir, f'system_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)
        
        # Create detailed formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)d]'
        )
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        
    async def start_monitoring(self):
        """Start system monitoring."""
        self._is_running = True
        await asyncio.gather(
            self._monitor_heartbeats(),
            self._check_component_health()
        )
        
    async def _monitor_heartbeats(self):
        """Monitor system heartbeats."""
        while self._is_running:
            try:
                current_time = datetime.now()
                for component, last_time in self.last_heartbeat.items():
                    if (current_time - last_time).total_seconds() > self.max_silence:
                        logging.error(f"Component {component} hasn't reported for {self.max_silence} seconds!")
                        self.component_status[component] = "UNRESPONSIVE"
                
                await asyncio.sleep(self.heartbeat_interval)
                
            except Exception as e:
                logging.error(f"Error in heartbeat monitoring: {e}")
                await asyncio.sleep(5)
                
    async def _check_component_health(self):
        """Check health of system components."""
        while self._is_running:
            try:
                # Log memory usage
                import psutil
                process = psutil.Process()
                mem_info = process.memory_info()
                logging.info(f"Memory usage: {mem_info.rss / 1024 / 1024:.2f} MB")
                
                # Log component status
                for component, status in self.component_status.items():
                    logging.info(f"Component {component} status: {status}")
                
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logging.error(f"Error in health check: {e}")
                await asyncio.sleep(5)
                
    def record_heartbeat(self, component: str):
        """Record a heartbeat from a system component."""
        self.last_heartbeat[component] = datetime.now()
        self.component_status[component] = "ACTIVE"
        
    def stop_monitoring(self):
        """Stop system monitoring."""
        self._is_running = False
        logging.info("System monitoring stopped")
