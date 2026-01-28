"""
Script 1: Create Base Snapshot
Downloads full base tables for a package and creates initial snapshot
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from common_utils import (
    ConfigManager, WatermarkManager, DirectoryManager,
    SnowflakeConnector, RunMetrics, get_current_timestamp_est,
    setup_logging
)


class BaseSnapshotCreator:
    """Create base snapshots for package tables"""
    
    def __init__(self, config_path, package_name):
        self.config = ConfigManager.load_config(config_path)
        self.package_name = package_name
        
        # Initialize directory manager
        self.dir_manager = DirectoryManager(self.config['filesystem']['data_root'])
        self.dir_manager.ensure_structure()
        
        # Setup logging
        self.logger = setup_logging(
            self.dir_manager.get_logs_dir(),
            f'base_snapshot_{package_name}',
            level=getattr(__import__('logging'), self.config['logging']['level'])
        )
        
        # Initialize managers
        self.watermark_mgr = WatermarkManager(self.dir_manager.get_metadata_dir())
        self.metrics_mgr = RunMetrics(self.dir_manager.get_metadata_dir())
        
        # Snowflake connector
        self.sf_connector = SnowflakeConnector(self.config)
        self.conn = None
        
    def get_package_tables(self):
        """
        Get list of tables for the package from loadtracker
        
        Returns:
            list: List of table names
        """
        loadtracker_table = self.config['snowflake']['loadtracker_table']
        
        # If loadtracker table doesn't contain schema qualifier, use current schema
        if '.' not in loadtracker_table:
            # Use simple table name (already in current schema)
            query = f"""
            SELECT DISTINCT TABLENAME
            FROM {loadtracker_table}
            WHERE PACKAGENAME = %s
            ORDER BY TABLENAME
            """
        else:
            # Use fully qualified table name
            query = f"""
            SELECT DISTINCT TABLENAME
            FROM {loadtracker_table}
            WHERE PACKAGENAME = %s
            ORDER BY TABLENAME
            """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (self.package_name,))
            
            tables = [row[0] for row in cursor.fetchall()]
            
            self.logger.info(f"Found {len(tables)} tables for package '{self.package_name}'")
            
            if not tables:
                self.logger.warning(f"No tables found for package '{self.package_name}' in loadtracker")
            
            return tables
            
        except Exception as e:
            self.logger.error(f"Failed to query loadtracker: {str(e)}")
            raise
    
    def create_table_snapshot(self, table_name):
        """
        Create base snapshot for a single table
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            int: Number of rows in snapshot
        """
        table_start_time = datetime.now()
        
        self.logger.info(f"Creating base snapshot for table: {table_name}")
        
        try:
            # Ensure directory structure
            self.dir_manager.ensure_table_structure(self.package_name, table_name)
            
            # Define file paths
            table_dir = self.dir_manager.get_package_table_dir(self.package_name, table_name)
            stage_path = f"@~/base_snapshot_{self.package_name}_{table_name}/"
            
            # Get database and schema from config for fully qualified table name
            database = self.config['snowflake']['database']
            schema = self.config['snowflake']['schema']
            qualified_table_name = f"{database}.{schema}.{table_name}"
            
            # Step 1: Export table to Snowflake internal stage as Parquet
            self.logger.info(f"Exporting {table_name} to Snowflake stage...")
            self.logger.info(f"Using fully qualified table: {qualified_table_name}")
            
            export_query = f"""
            COPY INTO {stage_path}
            FROM {qualified_table_name}
            FILE_FORMAT = (
                TYPE = PARQUET
                COMPRESSION = '{self.config['parquet']['compression'].upper()}'
            )
            MAX_FILE_SIZE = {self.config['parquet']['max_file_size_mb'] * 1024 * 1024}
            OVERWRITE = TRUE
            HEADER = TRUE
            """
            
            cursor = self.conn.cursor()
            cursor.execute(export_query)
            result = cursor.fetchone()
            
            self.logger.info(f"Stage export result: {result}")
            
            # Step 2: Download from stage to local filesystem
            self.logger.info(f"Downloading files from stage to local filesystem...")
            
            local_temp_dir = table_dir / 'temp'
            local_temp_dir.mkdir(parents=True, exist_ok=True)
            
            get_query = f"GET {stage_path} 'file://{str(local_temp_dir).replace(chr(92), '/')}'"
            
            cursor.execute(get_query)
            get_result = cursor.fetchall()
            
            self.logger.info(f"Downloaded {len(get_result)} file(s)")
            
            # Step 3: Combine Parquet files if multiple
            parquet_files = list(local_temp_dir.glob('*.parquet')) + list(local_temp_dir.glob('*.gz.parquet'))
            
            if not parquet_files:
                raise Exception(f"No Parquet files found for {table_name}")
            
            self.logger.info(f"Combining {len(parquet_files)} Parquet file(s)...")
            
            # Read all parquet files
            tables_to_combine = []
            for pq_file in parquet_files:
                table = pq.read_table(pq_file)
                tables_to_combine.append(table)
            
            # Combine into single table
            combined_table = pa.concat_tables(tables_to_combine)
            row_count = len(combined_table)
            
            self.logger.info(f"Combined table has {row_count:,} rows")
            
            # Step 4: Write base_snapshot.parquet
            base_snapshot_path = table_dir / 'base_snapshot.parquet'
            pq.write_table(combined_table, base_snapshot_path, compression='snappy')
            
            self.logger.info(f"Created base_snapshot.parquet: {base_snapshot_path}")
            
            # Step 5: Create current_state.parquet (identical to base_snapshot initially)
            current_state_path = table_dir / 'current_state.parquet'
            pq.write_table(combined_table, current_state_path, compression='snappy')
            
            self.logger.info(f"Created current_state.parquet: {current_state_path}")
            
            # Step 6: Cleanup
            self.logger.info("Cleaning up temporary files...")
            
            # Remove local temp files
            for pq_file in parquet_files:
                pq_file.unlink()
            local_temp_dir.rmdir()
            
            # Remove files from Snowflake stage
            cursor.execute(f"REMOVE {stage_path}")
            
            # Record table metrics
            table_end_time = datetime.now()
            self.metrics_mgr.record_table_metrics(
                package_name=self.package_name,
                table_name=table_name,
                run_type='base_snapshot',
                start_time=table_start_time,
                end_time=table_end_time,
                rows_processed=row_count
            )
            
            duration = (table_end_time - table_start_time).total_seconds()
            self.logger.info(f"Completed {table_name} in {duration:.2f}s ({row_count:,} rows)")
            
            return row_count
            
        except Exception as e:
            self.logger.error(f"Failed to create snapshot for {table_name}: {str(e)}")
            raise
    
    def run(self):
        """Execute base snapshot creation for all tables in package"""
        
        run_start_time = get_current_timestamp_est()
        
        self.logger.info("="*80)
        self.logger.info(f"Starting Base Snapshot Creation for Package: {self.package_name}")
        self.logger.info(f"Timestamp: {run_start_time}")
        self.logger.info("="*80)
        
        try:
            # Connect to Snowflake
            self.logger.info("Connecting to Snowflake...")
            self.conn = self.sf_connector.connect()
            self.logger.info("Connected to Snowflake successfully")
            
            # Get tables for package
            tables = self.get_package_tables()
            
            if not tables:
                self.logger.error("No tables found for package. Exiting.")
                return 1
            
            # Process each table
            total_rows = 0
            successful_tables = 0
            failed_tables = []
            
            for idx, table_name in enumerate(tables, 1):
                self.logger.info(f"\nProcessing table {idx}/{len(tables)}: {table_name}")
                
                try:
                    row_count = self.create_table_snapshot(table_name)
                    total_rows += row_count
                    successful_tables += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to process {table_name}: {str(e)}")
                    failed_tables.append(table_name)
            
            # Update watermark
            run_end_time = get_current_timestamp_est()
            
            self.watermark_mgr.set_watermark(
                package_name=self.package_name,
                timestamp=run_end_time,
                run_type='base_snapshot',
                status='success' if not failed_tables else 'partial',
                tables_count=successful_tables
            )
            
            # Record run metrics
            self.metrics_mgr.record_run(
                package_name=self.package_name,
                run_type='base_snapshot',
                start_time=run_start_time,
                end_time=run_end_time,
                tables_processed=successful_tables,
                total_rows=total_rows,
                status='success' if not failed_tables else 'partial',
                error_message=f"Failed tables: {', '.join(failed_tables)}" if failed_tables else None
            )
            
            # Summary
            duration = (run_end_time - run_start_time).total_seconds()
            
            self.logger.info("\n" + "="*80)
            self.logger.info("BASE SNAPSHOT CREATION SUMMARY")
            self.logger.info("="*80)
            self.logger.info(f"Package: {self.package_name}")
            self.logger.info(f"Total Tables: {len(tables)}")
            self.logger.info(f"Successful: {successful_tables}")
            self.logger.info(f"Failed: {len(failed_tables)}")
            self.logger.info(f"Total Rows: {total_rows:,}")
            self.logger.info(f"Duration: {duration:.2f}s")
            self.logger.info(f"Watermark: {run_end_time}")
            
            if failed_tables:
                self.logger.warning(f"Failed tables: {', '.join(failed_tables)}")
            
            self.logger.info("="*80)
            
            return 0 if not failed_tables else 1
            
        except Exception as e:
            run_end_time = get_current_timestamp_est()
            
            self.logger.error(f"Base snapshot creation failed: {str(e)}")
            
            # Record failed run
            self.metrics_mgr.record_run(
                package_name=self.package_name,
                run_type='base_snapshot',
                start_time=run_start_time,
                end_time=run_end_time,
                tables_processed=0,
                total_rows=0,
                status='failed',
                error_message=str(e)
            )
            
            return 1
            
        finally:
            # Close Snowflake connection
            if self.conn:
                self.sf_connector.close()
                self.logger.info("Snowflake connection closed")


def main():
    """Main execution function"""
    
    parser = argparse.ArgumentParser(
        description='Create base snapshot for a package from Snowflake'
    )
    parser.add_argument(
        '--package',
        required=True,
        help='Package name to process'
    )
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    
    args = parser.parse_args()
    
    # Create and run base snapshot creator
    creator = BaseSnapshotCreator(args.config, args.package)
    return creator.run()


if __name__ == "__main__":
    sys.exit(main())
