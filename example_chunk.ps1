"""
Alternative Script 2: Create Base Snapshot (Chunked Method)
Downloads data in batches to handle large tables efficiently
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from common_utils import (
    ConfigManager, WatermarkManager, DirectoryManager,
    SnowflakeConnector, RunMetrics, get_current_timestamp_est,
    setup_logging
)


class BaseSnapshotCreatorChunked:
    """Create base snapshots by querying Snowflake in chunks"""
    
    def __init__(self, config_path, package_name, chunk_size=50000):
        self.config = ConfigManager.load_config(config_path)
        self.package_name = package_name
        self.chunk_size = chunk_size  # Rows per chunk
        
        # Sanitize package name for file system
        self.package_name_safe = package_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
        
        # Initialize directory manager
        self.dir_manager = DirectoryManager(self.config['filesystem']['data_root'])
        self.dir_manager.ensure_structure()
        
        # Setup logging
        self.logger = setup_logging(
            self.dir_manager.get_logs_dir(),
            f'base_snapshot_{self.package_name_safe}',
            level=getattr(__import__('logging'), self.config['logging']['level'])
        )
        
        # Initialize managers
        self.watermark_mgr = WatermarkManager(self.dir_manager.get_metadata_dir())
        self.metrics_mgr = RunMetrics(self.dir_manager.get_metadata_dir())
        
        # Snowflake connector
        self.sf_connector = SnowflakeConnector(self.config)
        self.conn = None
        
    def get_package_tables(self):
        """Get list of tables for the package from loadtracker"""
        loadtracker_table = self.config['snowflake']['loadtracker_table']
        
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
    
    def create_table_snapshot_chunked(self, table_name):
        """
        Create base snapshot by querying table in chunks
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            int: Number of rows in snapshot
        """
        table_start_time = datetime.now()
        
        self.logger.info(f"Creating base snapshot for table: {table_name}")
        
        try:
            # Ensure directory structure
            self.dir_manager.ensure_table_structure(self.package_name_safe, table_name)
            
            # Define file paths
            table_dir = self.dir_manager.get_package_table_dir(self.package_name_safe, table_name)
            
            # Get fully qualified table name
            database = self.config['snowflake']['database']
            schema = self.config['snowflake']['schema']
            qualified_table_name = f"{database}.{schema}.{table_name}"
            
            self.logger.info(f"Querying table: {qualified_table_name}")
            self.logger.info(f"Using chunk size: {self.chunk_size:,} rows")
            
            # Query the table with cursor
            query = f"SELECT * FROM {qualified_table_name}"
            
            cursor = self.conn.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            self.logger.info(f"Fetching data in chunks of {self.chunk_size:,} rows...")
            
            # Fetch data in chunks and write to parquet files
            chunk_files = []
            chunk_num = 0
            total_rows = 0
            
            while True:
                # Fetch chunk
                rows = cursor.fetchmany(self.chunk_size)
                
                if not rows:
                    break  # No more data
                
                chunk_num += 1
                chunk_row_count = len(rows)
                total_rows += chunk_row_count
                
                self.logger.info(f"Chunk {chunk_num}: Fetched {chunk_row_count:,} rows (Total: {total_rows:,})")
                
                # Create DataFrame for this chunk
                df_chunk = pd.DataFrame(rows, columns=columns)
                
                # Convert to PyArrow Table
                pa_table = pa.Table.from_pandas(df_chunk)
                
                # Write chunk to temporary parquet file
                chunk_file = table_dir / f'temp_chunk_{chunk_num}.parquet'
                pq.write_table(pa_table, chunk_file, compression='snappy')
                chunk_files.append(chunk_file)
                
                self.logger.info(f"Chunk {chunk_num} written to {chunk_file.name}")
            
            self.logger.info(f"Fetched total of {total_rows:,} rows in {chunk_num} chunks")
            
            if total_rows == 0:
                self.logger.warning(f"Table {table_name} is empty (0 rows)")
                # Create empty DataFrame with proper columns
                df = pd.DataFrame(columns=columns)
                pa_table = pa.Table.from_pandas(df)
                
                # Write empty parquet files
                base_snapshot_path = table_dir / 'base_snapshot.parquet'
                pq.write_table(pa_table, base_snapshot_path, compression='snappy')
                
                current_state_path = table_dir / 'current_state.parquet'
                pq.write_table(pa_table, current_state_path, compression='snappy')
                
            else:
                # Combine all chunks into final parquet files
                self.logger.info(f"Combining {len(chunk_files)} chunks into final parquet files...")
                
                # Read all chunk files
                tables_to_combine = []
                for chunk_file in chunk_files:
                    chunk_table = pq.read_table(chunk_file)
                    tables_to_combine.append(chunk_table)
                
                # Combine into single table
                combined_table = pa.concat_tables(tables_to_combine)
                
                self.logger.info(f"Combined table has {len(combined_table):,} rows")
                
                # Write base_snapshot.parquet
                base_snapshot_path = table_dir / 'base_snapshot.parquet'
                pq.write_table(combined_table, base_snapshot_path, compression='snappy')
                
                self.logger.info(f"Created base_snapshot.parquet: {base_snapshot_path}")
                
                # Create current_state.parquet
                current_state_path = table_dir / 'current_state.parquet'
                pq.write_table(combined_table, current_state_path, compression='snappy')
                
                self.logger.info(f"Created current_state.parquet: {current_state_path}")
                
                # Cleanup: Remove temporary chunk files
                self.logger.info("Removing temporary chunk files...")
                for chunk_file in chunk_files:
                    chunk_file.unlink()
                
                self.logger.info("Cleanup complete")
            
            # Record table metrics
            table_end_time = datetime.now()
            self.metrics_mgr.record_table_metrics(
                package_name=self.package_name_safe,
                table_name=table_name,
                run_type='base_snapshot',
                start_time=table_start_time,
                end_time=table_end_time,
                rows_processed=total_rows
            )
            
            duration = (table_end_time - table_start_time).total_seconds()
            self.logger.info(f"Completed {table_name} in {duration:.2f}s ({total_rows:,} rows)")
            
            return total_rows
            
        except Exception as e:
            self.logger.error(f"Failed to create snapshot for {table_name}: {str(e)}")
            raise
    
    def run(self):
        """Execute base snapshot creation for all tables in package"""
        
        run_start_time = get_current_timestamp_est()
        
        self.logger.info("="*80)
        self.logger.info(f"Starting Base Snapshot Creation for Package: {self.package_name}")
        self.logger.info(f"Method: Chunked (batch size: {self.chunk_size:,} rows)")
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
                    row_count = self.create_table_snapshot_chunked(table_name)
                    total_rows += row_count
                    successful_tables += 1
                    
                except Exception as e:
                    self.logger.error(f"Failed to process {table_name}: {str(e)}")
                    failed_tables.append(table_name)
            
            # Update watermark
            run_end_time = get_current_timestamp_est()
            
            self.watermark_mgr.set_watermark(
                package_name=self.package_name_safe,
                timestamp=run_end_time,
                run_type='base_snapshot',
                status='success' if not failed_tables else 'partial',
                tables_count=successful_tables
            )
            
            # Record run metrics
            self.metrics_mgr.record_run(
                package_name=self.package_name_safe,
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
                package_name=self.package_name_safe,
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
        description='Create base snapshot for a package from Snowflake (Chunked Method)'
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
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=50000,
        help='Number of rows to fetch per chunk (default: 50000)'
    )
    
    args = parser.parse_args()
    
    # Create and run base snapshot creator
    creator = BaseSnapshotCreatorChunked(args.config, args.package, args.chunk_size)
    return creator.run()


if __name__ == "__main__":
    sys.exit(main())
