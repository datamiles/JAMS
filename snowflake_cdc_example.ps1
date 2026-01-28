"""
Alternative Script 3: Create Base Snapshot (Robust Chunked with Retry)
Downloads data in batches with connection retry logic
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time

from common_utils import (
    ConfigManager, WatermarkManager, DirectoryManager,
    SnowflakeConnector, RunMetrics, get_current_timestamp_est,
    setup_logging
)


class BaseSnapshotCreatorRobust:
    """Create base snapshots with robust error handling and retry logic"""
    
    def __init__(self, config_path, package_name, chunk_size=10000):
        self.config = ConfigManager.load_config(config_path)
        self.package_name = package_name
        self.chunk_size = chunk_size
        
        # Sanitize package name
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
        
    def reconnect(self):
        """Reconnect to Snowflake"""
        try:
            if self.conn:
                self.conn.close()
        except:
            pass
        
        self.logger.info("Reconnecting to Snowflake...")
        self.conn = self.sf_connector.connect()
        self.logger.info("Reconnected successfully")
    
    def get_package_tables(self):
        """Get list of tables for the package from loadtracker"""
        loadtracker_table = self.config['snowflake']['loadtracker_table']
        
        query = f"""
        SELECT DISTINCT TABLENAME
        FROM {loadtracker_table}
        WHERE PACKAGENAME = %s
        ORDER BY TABLENAME
        """
        
        cursor = self.conn.cursor()
        cursor.execute(query, (self.package_name,))
        tables = [row[0] for row in cursor.fetchall()]
        
        self.logger.info(f"Found {len(tables)} tables for package '{self.package_name}'")
        return tables
    
    def get_row_count(self, qualified_table_name):
        """Get total row count for a table"""
        try:
            query = f"SELECT COUNT(*) FROM {qualified_table_name}"
            cursor = self.conn.cursor()
            cursor.execute(query)
            row_count = cursor.fetchone()[0]
            return row_count
        except Exception as e:
            self.logger.warning(f"Could not get row count: {e}")
            return None
    
    def create_table_snapshot_with_offset(self, table_name):
        """
        Create base snapshot using OFFSET/LIMIT for chunking
        More reliable than cursor.fetchmany for long-running queries
        """
        table_start_time = datetime.now()
        
        self.logger.info(f"Creating base snapshot for table: {table_name}")
        
        try:
            # Ensure directory structure
            self.dir_manager.ensure_table_structure(self.package_name_safe, table_name)
            table_dir = self.dir_manager.get_package_table_dir(self.package_name_safe, table_name)
            
            # Get fully qualified table name
            database = self.config['snowflake']['database']
            schema = self.config['snowflake']['schema']
            qualified_table_name = f"{database}.{schema}.{table_name}"
            
            self.logger.info(f"Table: {qualified_table_name}")
            
            # Get row count
            self.logger.info("Getting row count...")
            total_row_count = self.get_row_count(qualified_table_name)
            if total_row_count:
                self.logger.info(f"Total rows in table: {total_row_count:,}")
                num_chunks = (total_row_count // self.chunk_size) + 1
                self.logger.info(f"Will process in ~{num_chunks} chunks of {self.chunk_size:,} rows")
            
            # Get column names
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT * FROM {qualified_table_name} LIMIT 1")
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch data in chunks using OFFSET/LIMIT
            chunk_files = []
            chunk_num = 0
            total_rows = 0
            offset = 0
            max_retries = 3
            
            while True:
                retry_count = 0
                chunk_fetched = False
                
                while retry_count < max_retries and not chunk_fetched:
                    try:
                        # Query with OFFSET and LIMIT
                        query = f"""
                        SELECT * FROM {qualified_table_name}
                        ORDER BY 1
                        LIMIT {self.chunk_size}
                        OFFSET {offset}
                        """
                        
                        self.logger.info(f"Fetching chunk {chunk_num + 1} (offset: {offset:,})...")
                        
                        # Reconnect for each chunk to avoid timeout
                        if chunk_num > 0 and chunk_num % 5 == 0:
                            self.logger.info("Reconnecting to prevent timeout...")
                            self.reconnect()
                            cursor = self.conn.cursor()
                        
                        cursor = self.conn.cursor()
                        cursor.execute(query)
                        rows = cursor.fetchall()
                        
                        if not rows:
                            self.logger.info("No more rows to fetch")
                            chunk_fetched = True
                            break
                        
                        chunk_num += 1
                        chunk_row_count = len(rows)
                        total_rows += chunk_row_count
                        
                        self.logger.info(f"Chunk {chunk_num}: Fetched {chunk_row_count:,} rows (Total: {total_rows:,})")
                        
                        # Create DataFrame
                        df_chunk = pd.DataFrame(rows, columns=columns)
                        pa_table = pa.Table.from_pandas(df_chunk)
                        
                        # Write chunk
                        chunk_file = table_dir / f'temp_chunk_{chunk_num}.parquet'
                        pq.write_table(pa_table, chunk_file, compression='snappy')
                        chunk_files.append(chunk_file)
                        
                        offset += chunk_row_count
                        chunk_fetched = True
                        
                        # If we got fewer rows than chunk_size, we're done
                        if chunk_row_count < self.chunk_size:
                            self.logger.info("Received partial chunk - end of table reached")
                            break
                        
                    except Exception as e:
                        retry_count += 1
                        self.logger.error(f"Error fetching chunk: {e}")
                        
                        if retry_count < max_retries:
                            self.logger.info(f"Retry {retry_count}/{max_retries}...")
                            time.sleep(5)
                            self.reconnect()
                        else:
                            self.logger.error(f"Failed after {max_retries} retries")
                            raise
                
                if not chunk_fetched or (rows and len(rows) < self.chunk_size):
                    break
            
            self.logger.info(f"Fetched total of {total_rows:,} rows in {chunk_num} chunks")
            
            if total_rows == 0:
                # Empty table
                df = pd.DataFrame(columns=columns)
                pa_table = pa.Table.from_pandas(df)
                
                base_snapshot_path = table_dir / 'base_snapshot.parquet'
                pq.write_table(pa_table, base_snapshot_path, compression='snappy')
                
                current_state_path = table_dir / 'current_state.parquet'
                pq.write_table(pa_table, current_state_path, compression='snappy')
            else:
                # Combine chunks
                self.logger.info(f"Combining {len(chunk_files)} chunks...")
                
                tables_to_combine = []
                for chunk_file in chunk_files:
                    chunk_table = pq.read_table(chunk_file)
                    tables_to_combine.append(chunk_table)
                
                combined_table = pa.concat_tables(tables_to_combine)
                self.logger.info(f"Combined table has {len(combined_table):,} rows")
                
                # Write final files
                base_snapshot_path = table_dir / 'base_snapshot.parquet'
                pq.write_table(combined_table, base_snapshot_path, compression='snappy')
                self.logger.info(f"Created: {base_snapshot_path}")
                
                current_state_path = table_dir / 'current_state.parquet'
                pq.write_table(combined_table, current_state_path, compression='snappy')
                self.logger.info(f"Created: {current_state_path}")
                
                # Cleanup
                self.logger.info("Removing temporary files...")
                for chunk_file in chunk_files:
                    chunk_file.unlink()
            
            # Record metrics
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
        """Execute base snapshot creation"""
        
        run_start_time = get_current_timestamp_est()
        
        self.logger.info("="*80)
        self.logger.info(f"Starting Base Snapshot Creation")
        self.logger.info(f"Package: {self.package_name}")
        self.logger.info(f"Chunk size: {self.chunk_size:,} rows")
        self.logger.info(f"Timestamp: {run_start_time}")
        self.logger.info("="*80)
        
        try:
            # Connect
            self.logger.info("Connecting to Snowflake...")
            self.conn = self.sf_connector.connect()
            self.logger.info("Connected successfully")
            
            # Get tables
            tables = self.get_package_tables()
            
            if not tables:
                self.logger.error("No tables found. Exiting.")
                return 1
            
            # Process each table
            total_rows = 0
            successful_tables = 0
            failed_tables = []
            
            for idx, table_name in enumerate(tables, 1):
                self.logger.info(f"\n{'='*80}")
                self.logger.info(f"Processing table {idx}/{len(tables)}: {table_name}")
                self.logger.info(f"{'='*80}")
                
                try:
                    row_count = self.create_table_snapshot_with_offset(table_name)
                    total_rows += row_count
                    successful_tables += 1
                except Exception as e:
                    self.logger.error(f"Failed: {str(e)}")
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
            
            # Record metrics
            self.metrics_mgr.record_run(
                package_name=self.package_name_safe,
                run_type='base_snapshot',
                start_time=run_start_time,
                end_time=run_end_time,
                tables_processed=successful_tables,
                total_rows=total_rows,
                status='success' if not failed_tables else 'partial',
                error_message=f"Failed: {', '.join(failed_tables)}" if failed_tables else None
            )
            
            # Summary
            duration = (run_end_time - run_start_time).total_seconds()
            
            self.logger.info("\n" + "="*80)
            self.logger.info("SUMMARY")
            self.logger.info("="*80)
            self.logger.info(f"Package: {self.package_name}")
            self.logger.info(f"Tables: {len(tables)}")
            self.logger.info(f"Successful: {successful_tables}")
            self.logger.info(f"Failed: {len(failed_tables)}")
            self.logger.info(f"Total Rows: {total_rows:,}")
            self.logger.info(f"Duration: {duration:.2f}s")
            
            if failed_tables:
                self.logger.warning(f"Failed: {', '.join(failed_tables)}")
            
            self.logger.info("="*80)
            
            return 0 if not failed_tables else 1
            
        except Exception as e:
            self.logger.error(f"Failed: {str(e)}")
            return 1
            
        finally:
            if self.conn:
                self.sf_connector.close()


def main():
    parser = argparse.ArgumentParser(
        description='Create base snapshot (Robust method with retry)'
    )
    parser.add_argument('--package', required=True, help='Package name')
    parser.add_argument('--config', default='config.yaml', help='Config file')
    parser.add_argument('--chunk-size', type=int, default=10000, 
                       help='Rows per chunk (default: 10000)')
    
    args = parser.parse_args()
    
    creator = BaseSnapshotCreatorRobust(args.config, args.package, args.chunk_size)
    return creator.run()


if __name__ == "__main__":
    sys.exit(main())
