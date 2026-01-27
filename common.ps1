"""
Common Utilities Module
Shared functions for Snowflake to On-Prem Parquet CDC Solution
"""

import json
import yaml
import logging
import pytz
from datetime import datetime
from pathlib import Path
import snowflake.connector


class ConfigManager:
    """Configuration management utilities"""
    
    @staticmethod
    def load_config(config_path='config.yaml'):
        """Load YAML configuration file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    @staticmethod
    def load_json(json_path):
        """Load JSON file"""
        with open(json_path, 'r') as f:
            return json.load(f)
    
    @staticmethod
    def save_json(data, json_path):
        """Save data to JSON file"""
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=2)


class WatermarkManager:
    """Manage package-level watermarks"""
    
    def __init__(self, metadata_dir):
        self.watermark_file = Path(metadata_dir) / 'watermarks.json'
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        """Create watermarks file if it doesn't exist"""
        if not self.watermark_file.exists():
            initial_data = {
                "description": "Package-level watermarks for CDC processing",
                "watermarks": {}
            }
            ConfigManager.save_json(initial_data, self.watermark_file)
    
    def get_watermark(self, package_name):
        """
        Get watermark for a package
        
        Returns:
            datetime or None: Last update timestamp
        """
        data = ConfigManager.load_json(self.watermark_file)
        
        if package_name in data['watermarks']:
            timestamp_str = data['watermarks'][package_name]['last_update_timestamp']
            return datetime.fromisoformat(timestamp_str)
        
        return None
    
    def set_watermark(self, package_name, timestamp, run_type, status, tables_count):
        """
        Set watermark for a package
        
        Args:
            package_name (str): Package name
            timestamp (datetime): Update timestamp
            run_type (str): Type of run (base_snapshot, incremental_update)
            status (str): Run status (success, failed)
            tables_count (int): Number of tables processed
        """
        data = ConfigManager.load_json(self.watermark_file)
        
        data['watermarks'][package_name] = {
            'last_update_timestamp': timestamp.isoformat(),
            'last_run_type': run_type,
            'last_run_status': status,
            'tables_count': tables_count
        }
        
        ConfigManager.save_json(data, self.watermark_file)


class PKManager:
    """Manage primary key mappings"""
    
    def __init__(self, pk_mappings_path):
        self.pk_mappings_path = Path(pk_mappings_path)
        self.pk_mappings = self._load_mappings()
    
    def _load_mappings(self):
        """Load primary key mappings"""
        if not self.pk_mappings_path.exists():
            raise FileNotFoundError(
                f"Primary key mappings file not found: {self.pk_mappings_path}\n"
                "Please run extract_primary_keys.py first or create the file manually."
            )
        
        data = ConfigManager.load_json(self.pk_mappings_path)
        return data.get('tables', {})
    
    def get_primary_keys(self, table_name):
        """
        Get primary key columns for a table
        
        Args:
            table_name (str): Table name
            
        Returns:
            list: List of primary key column names
        """
        if table_name not in self.pk_mappings:
            raise ValueError(
                f"Primary key mapping not found for table: {table_name}\n"
                "Please update pk_mappings.json with this table's primary keys."
            )
        
        return self.pk_mappings[table_name]['primary_keys']


class DirectoryManager:
    """Manage directory structure"""
    
    def __init__(self, data_root):
        self.data_root = Path(data_root)
    
    def ensure_structure(self):
        """Create directory structure if it doesn't exist"""
        # Main directories
        (self.data_root / 'packages').mkdir(parents=True, exist_ok=True)
        (self.data_root / 'archives').mkdir(parents=True, exist_ok=True)
        (self.data_root / 'metadata').mkdir(parents=True, exist_ok=True)
        (self.data_root / 'logs').mkdir(parents=True, exist_ok=True)
    
    def get_package_table_dir(self, package_name, table_name):
        """Get directory path for a specific package/table"""
        return self.data_root / 'packages' / package_name / table_name
    
    def get_archive_dir(self, package_name, table_name, archive_type):
        """
        Get archive directory path
        
        Args:
            package_name (str): Package name
            table_name (str): Table name
            archive_type (str): 'states' or 'cdc'
        """
        return self.data_root / 'archives' / package_name / table_name / archive_type
    
    def get_metadata_dir(self):
        """Get metadata directory path"""
        return self.data_root / 'metadata'
    
    def get_logs_dir(self):
        """Get logs directory path"""
        return self.data_root / 'logs'
    
    def ensure_table_structure(self, package_name, table_name):
        """Ensure directory structure exists for a table"""
        # Package table directory
        table_dir = self.get_package_table_dir(package_name, table_name)
        table_dir.mkdir(parents=True, exist_ok=True)
        
        # Archive directories
        states_archive = self.get_archive_dir(package_name, table_name, 'states')
        states_archive.mkdir(parents=True, exist_ok=True)
        
        cdc_archive = self.get_archive_dir(package_name, table_name, 'cdc')
        cdc_archive.mkdir(parents=True, exist_ok=True)


class SnowflakeConnector:
    """Snowflake connection management"""
    
    def __init__(self, config):
        self.config = config
        self.conn = None
    
    def connect(self):
        """Establish Snowflake connection"""
        try:
            sf_config = self.config['snowflake']
            
            self.conn = snowflake.connector.connect(
                account=sf_config['account'],
                user=sf_config['user'],
                password=sf_config['password'],
                warehouse=sf_config['warehouse'],
                database=sf_config['database'],
                schema=sf_config['schema'],
                role=sf_config.get('role')
            )
            
            return self.conn
            
        except Exception as e:
            raise Exception(f"Failed to connect to Snowflake: {str(e)}")
    
    def close(self):
        """Close Snowflake connection"""
        if self.conn:
            self.conn.close()


class RunMetrics:
    """Track and store run metrics"""
    
    def __init__(self, metadata_dir):
        self.metrics_file = Path(metadata_dir) / 'run_metrics.json'
        self._ensure_file_exists()
    
    def _ensure_file_exists(self):
        """Create metrics file if it doesn't exist"""
        if not self.metrics_file.exists():
            initial_data = {
                "description": "Run metrics and performance tracking",
                "runs": []
            }
            ConfigManager.save_json(initial_data, self.metrics_file)
    
    def record_run(self, package_name, run_type, start_time, end_time, 
                   tables_processed, total_rows, status, error_message=None):
        """
        Record metrics for a run
        
        Args:
            package_name (str): Package name
            run_type (str): Type of run (base_snapshot, incremental_update)
            start_time (datetime): Start timestamp
            end_time (datetime): End timestamp
            tables_processed (int): Number of tables processed
            total_rows (int): Total rows processed
            status (str): Run status (success, failed, partial)
            error_message (str): Error message if failed
        """
        data = ConfigManager.load_json(self.metrics_file)
        
        duration_seconds = (end_time - start_time).total_seconds()
        
        run_record = {
            'package_name': package_name,
            'run_type': run_type,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'duration_seconds': duration_seconds,
            'duration_formatted': self._format_duration(duration_seconds),
            'tables_processed': tables_processed,
            'total_rows': total_rows,
            'status': status,
            'error_message': error_message
        }
        
        data['runs'].append(run_record)
        
        ConfigManager.save_json(data, self.metrics_file)
    
    def record_table_metrics(self, package_name, table_name, run_type, 
                            start_time, end_time, rows_processed, 
                            insert_count=0, update_count=0, delete_count=0):
        """
        Record metrics for individual table processing
        
        Args:
            package_name (str): Package name
            table_name (str): Table name
            run_type (str): Type of run
            start_time (datetime): Start timestamp
            end_time (datetime): End timestamp
            rows_processed (int): Number of rows processed
            insert_count (int): Number of inserts (CDC only)
            update_count (int): Number of updates (CDC only)
            delete_count (int): Number of deletes (CDC only)
        """
        table_metrics_file = Path(self.metrics_file.parent) / 'table_metrics.json'
        
        if not table_metrics_file.exists():
            initial_data = {
                "description": "Per-table processing metrics",
                "metrics": []
            }
            ConfigManager.save_json(initial_data, table_metrics_file)
        
        data = ConfigManager.load_json(table_metrics_file)
        
        duration_seconds = (end_time - start_time).total_seconds()
        
        metric_record = {
            'package_name': package_name,
            'table_name': table_name,
            'run_type': run_type,
            'timestamp': end_time.isoformat(),
            'duration_seconds': duration_seconds,
            'duration_formatted': self._format_duration(duration_seconds),
            'rows_processed': rows_processed,
            'insert_count': insert_count,
            'update_count': update_count,
            'delete_count': delete_count
        }
        
        data['metrics'].append(metric_record)
        
        ConfigManager.save_json(data, table_metrics_file)
    
    @staticmethod
    def _format_duration(seconds):
        """Format duration in human-readable format"""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        else:
            return f"{secs}s"


def get_current_timestamp_est():
    """Get current timestamp in EST timezone"""
    est = pytz.timezone('US/Eastern')
    return datetime.now(est)


def setup_logging(log_dir, log_name, level=logging.INFO):
    """
    Setup logging configuration
    
    Args:
        log_dir (Path): Directory for log files
        log_name (str): Log file name
        level: Logging level
        
    Returns:
        logger: Configured logger instance
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / f"{log_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Create logger
    logger = logging.getLogger(log_name)
    logger.setLevel(level)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
