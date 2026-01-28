"""
Script 2: Check Updates
Checks if any tables in the package have been updated since last watermark
"""

import argparse
import sys
from datetime import datetime

from common_utils import (
    ConfigManager, WatermarkManager, DirectoryManager,
    SnowflakeConnector, get_current_timestamp_est, setup_logging
)


class UpdateChecker:
    """Check for updates in package tables"""
    
    def __init__(self, config_path, package_name):
        self.config = ConfigManager.load_config(config_path)
        self.package_name = package_name
        
        # Sanitize package name for file system
        self.package_name_safe = package_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        
        # Initialize directory manager
        self.dir_manager = DirectoryManager(self.config['filesystem']['data_root'])
        self.dir_manager.ensure_structure()
        
        # Setup logging
        self.logger = setup_logging(
            self.dir_manager.get_logs_dir(),
            f'check_updates_{self.package_name_safe}',
            level=getattr(__import__('logging'), self.config['logging']['level'])
        )
        
        # Initialize watermark manager
        self.watermark_mgr = WatermarkManager(self.dir_manager.get_metadata_dir())
        
        # Snowflake connector
        self.sf_connector = SnowflakeConnector(self.config)
        self.conn = None
    
    def get_package_watermark(self):
        """
        Get the current watermark for the package
        
        Returns:
            datetime or None: Last update timestamp
        """
        watermark = self.watermark_mgr.get_watermark(self.package_name_safe)
        
        if watermark is None:
            self.logger.warning(f"No watermark found for package '{self.package_name}'")
            self.logger.warning("This suggests base snapshot has not been created yet.")
            self.logger.warning("Please run create_base_snapshot.py first.")
        
        return watermark
    
    def get_max_table_update_time(self):
        """
        Get the maximum LASTTABLEUPDATEDATETIME from loadtracker for this package
        
        Returns:
            datetime or None: Maximum update timestamp
        """
        loadtracker_table = self.config['snowflake']['loadtracker_table']
        
        query = f"""
        SELECT MAX(LASTTABLEUPDATEDATETIME) as MAX_UPDATE_TIME
        FROM {loadtracker_table}
        WHERE PACKAGENAME = %s
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (self.package_name,))
            
            result = cursor.fetchone()
            
            if result and result[0]:
                max_update_time = result[0]
                
                # If it's already a datetime object, use it directly
                if isinstance(max_update_time, datetime):
                    return max_update_time
                
                # Otherwise, try to parse it
                return datetime.fromisoformat(str(max_update_time))
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to query loadtracker: {str(e)}")
            raise
    
    def get_updated_tables(self, watermark):
        """
        Get list of tables that have been updated since the watermark
        
        Args:
            watermark (datetime): Watermark timestamp
            
        Returns:
            list: List of (table_name, update_time) tuples
        """
        loadtracker_table = self.config['snowflake']['loadtracker_table']
        
        query = f"""
        SELECT TABLENAME, LASTTABLEUPDATEDATETIME
        FROM {loadtracker_table}
        WHERE PACKAGENAME = %s
          AND LASTTABLEUPDATEDATETIME > %s
        ORDER BY LASTTABLEUPDATEDATETIME DESC
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (self.package_name, watermark))
            
            updated_tables = cursor.fetchall()
            
            return updated_tables
            
        except Exception as e:
            self.logger.error(f"Failed to query updated tables: {str(e)}")
            raise
    
    def run(self):
        """Execute update check"""
        
        check_time = get_current_timestamp_est()
        
        self.logger.info("="*80)
        self.logger.info(f"Checking for Updates - Package: {self.package_name}")
        self.logger.info(f"Check Time: {check_time}")
        self.logger.info("="*80)
        
        try:
            # Get package watermark
            self.logger.info("Retrieving package watermark...")
            watermark = self.get_package_watermark()
            
            if watermark is None:
                self.logger.error("Cannot proceed without a watermark. Exiting.")
                return 2  # Return code 2 indicates missing watermark
            
            self.logger.info(f"Package Watermark: {watermark}")
            
            # Connect to Snowflake
            self.logger.info("Connecting to Snowflake...")
            self.conn = self.sf_connector.connect()
            self.logger.info("✓ Connected to Snowflake")
            
            # Get max update time from loadtracker
            self.logger.info("Querying loadtracker for latest updates...")
            max_update_time = self.get_max_table_update_time()
            
            if max_update_time is None:
                self.logger.warning(f"No tables found for package '{self.package_name}' in loadtracker")
                self.logger.info("No updates available.")
                return 1  # Return code 1 indicates no updates
            
            self.logger.info(f"Max Table Update Time: {max_update_time}")
            
            # Compare watermark with max update time
            if max_update_time <= watermark:
                self.logger.info("\n" + "="*80)
                self.logger.info("NO UPDATES DETECTED")
                self.logger.info("="*80)
                self.logger.info(f"Max update time ({max_update_time}) <= Watermark ({watermark})")
                self.logger.info("No CDC processing needed.")
                self.logger.info("="*80)
                return 1  # Return code 1 indicates no updates
            
            # Updates detected - get details
            self.logger.info("\n✓ UPDATES DETECTED")
            self.logger.info(f"Max update time ({max_update_time}) > Watermark ({watermark})")
            
            # Get list of updated tables
            updated_tables = self.get_updated_tables(watermark)
            
            self.logger.info(f"\nTables with updates: {len(updated_tables)}")
            self.logger.info("\nUpdated Tables:")
            self.logger.info("-" * 80)
            
            for table_name, update_time in updated_tables:
                self.logger.info(f"  {table_name:40} | Updated: {update_time}")
            
            self.logger.info("-" * 80)
            
            self.logger.info("\n" + "="*80)
            self.logger.info("UPDATE CHECK SUMMARY")
            self.logger.info("="*80)
            self.logger.info(f"Package: {self.package_name}")
            self.logger.info(f"Watermark: {watermark}")
            self.logger.info(f"Max Update Time: {max_update_time}")
            self.logger.info(f"Tables with Updates: {len(updated_tables)}")
            self.logger.info(f"Result: PROCEED WITH CDC PROCESSING")
            self.logger.info("="*80)
            
            return 0  # Return code 0 indicates updates available
            
        except Exception as e:
            self.logger.error(f"Update check failed: {str(e)}")
            return 3  # Return code 3 indicates error
            
        finally:
            # Close Snowflake connection
            if self.conn:
                self.sf_connector.close()
                self.logger.info("Snowflake connection closed")


def main():
    """Main execution function"""
    
    parser = argparse.ArgumentParser(
        description='Check if package tables have updates since last watermark'
    )
    parser.add_argument(
        '--package',
        required=True,
        help='Package name to check'
    )
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    args = parser.parse_args()
    
    # Create and run update checker
    checker = UpdateChecker(args.config, args.package)
    return checker.run()


if __name__ == "__main__":
    sys.exit(main())
