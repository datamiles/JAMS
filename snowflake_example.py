"""
Script 3: Apply CDC Updates
Downloads CDC records and applies them to create new current state
"""

import argparse
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from shutil import copy2

from common_utils import (
    ConfigManager, WatermarkManager, PKManager, DirectoryManager,
    SnowflakeConnector, RunMetrics, get_current_timestamp_est,
    setup_logging
)


class CDCProcessor:
    """Process CDC updates for package tables"""
    
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
            f'apply_cdc_{self.package_name_safe}',
            level=getattr(__import__('logging'), self.config['logging']['level'])
        )
        
        # Initialize managers
        self.watermark_mgr = WatermarkManager(self.dir_manager.get_metadata_dir())
        self.metrics_mgr = RunMetrics(self.dir_manager.get_metadata_dir())
        
        # Primary key manager
        pk_mappings_path = self.dir_manager.get_metadata_dir() / 'pk_mappings.json'
        self.pk_mgr = PKManager(pk_mappings_path)
        
        # Snowflake connector
        self.sf_connector = SnowflakeConnector(self.config)
        self.conn = None
        
        # CDC configuration
        self.cdc_suffix = self.config['cdc'].get('cdc_suffix', '')
        self.cdc_prefix = self.config['cdc'].get('cdc_prefix', 'CDC_')
        self.metadata_action = self.config['cdc']['metadata_action']
        self.metadata_isupdate = self.config['cdc']['metadata_isupdate']
        self.insert_datetime = self.config['cdc']['insert_datetime']
    
    def get_package_tables(self):
        """
        Get list of tables for the package from loadtracker
        
        Returns:
            list: List of table names
        """
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
            
            return tables
            
        except Exception as e:
            self.logger.error(f"Failed to query loadtracker: {str(e)}")
            raise
    
    def check_cdc_table_exists(self, table_name):
        """
        Check if CDC table exists for the given base table
        
        Args:
            table_name (str): Base table name
            
        Returns:
            bool: True if CDC table exists
        """
        # Use prefix if configured, otherwise suffix
        if self.cdc_prefix:
            cdc_table_name = f"{self.cdc_prefix}{table_name}"
        else:
            cdc_table_name = f"{table_name}{self.cdc_suffix}"
        
        # Convert to uppercase (Snowflake default)
        cdc_table_name = cdc_table_name.upper()
        
        # Get CDC schema if configured, otherwise use current schema
        cdc_schema = self.config['cdc'].get('cdc_schema', self.config['snowflake']['schema'])
        cdc_schema = cdc_schema.upper()
        
        self.logger.info(f"Checking for CDC table: {cdc_schema}.{cdc_table_name}")
        
        query = """
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (cdc_schema, cdc_table_name))
            
            count = cursor.fetchone()[0]
            
            if count > 0:
                self.logger.info(f"CDC table exists: {cdc_schema}.{cdc_table_name}")
            else:
                self.logger.warning(f"CDC table not found: {cdc_schema}.{cdc_table_name}")
            
            return count > 0
            
        except Exception as e:
            self.logger.error(f"Failed to check CDC table existence: {str(e)}")
            return False
    
    def download_cdc_records(self, table_name, watermark):
        """
        Download CDC records from Snowflake for a table since watermark
        
        Args:
            table_name (str): Base table name
            watermark (datetime): Watermark timestamp
            
        Returns:
            Path or None: Path to downloaded CDC parquet file
        """
        # Use prefix if configured, otherwise suffix
        if self.cdc_prefix:
            cdc_table_name = f"{self.cdc_prefix}{table_name}"
        else:
            cdc_table_name = f"{table_name}{self.cdc_suffix}"
        
        # Convert to uppercase (Snowflake default)
        cdc_table_name = cdc_table_name.upper()
        
        # Get CDC schema if configured, otherwise use current schema
        cdc_schema = self.config['cdc'].get('cdc_schema', self.config['snowflake']['schema'])
        cdc_schema = cdc_schema.upper()
        
        # Get database
        database = self.config['snowflake']['database']
        database = database.upper()
        
        # Construct fully qualified CDC table name with quotes for identifiers
        qualified_cdc_table = f'"{database}"."{cdc_schema}"."{cdc_table_name}"'
        
        self.logger.info(f"Downloading CDC records for {table_name} since {watermark}...")
        self.logger.info(f"CDC table: {qualified_cdc_table}")
        
        try:
            # Define stage path
            stage_path = f"@~/cdc_{self.package_name}_{table_name}/"
            
            # Step 1: Export CDC records to Snowflake internal stage
            export_query = f"""
            COPY INTO {stage_path}
            FROM (
                SELECT *
                FROM {qualified_cdc_table}
                WHERE "{self.insert_datetime}" > %s
                ORDER BY "{self.insert_datetime}"
            )
            FILE_FORMAT = (
                TYPE = PARQUET
                COMPRESSION = '{self.config['parquet']['compression'].upper()}'
            )
            MAX_FILE_SIZE = {self.config['parquet']['max_file_size_mb'] * 1024 * 1024}
            OVERWRITE = TRUE
            HEADER = TRUE
            """
            
            cursor = self.conn.cursor()
            cursor.execute(export_query, (watermark,))
            result = cursor.fetchone()
            
            # Check if any records were exported
            if result and result[0] == 0:
                self.logger.info(f"No CDC records found for {table_name} since watermark")
                return None
            
            self.logger.info(f"Stage export result: {result}")
            
            # Step 2: Download from stage
            table_dir = self.dir_manager.get_package_table_dir(self.package_name, table_name)
            local_temp_dir = table_dir / 'cdc_temp'
            local_temp_dir.mkdir(parents=True, exist_ok=True)
            
            get_query = f"GET {stage_path} 'file://{str(local_temp_dir).replace(chr(92), '/')}'"
            
            cursor.execute(get_query)
            get_result = cursor.fetchall()
            
            self.logger.info(f"Downloaded {len(get_result)} CDC file(s)")
            
            # Step 3: Combine Parquet files if multiple
            parquet_files = list(local_temp_dir.glob('*.parquet')) + list(local_temp_dir.glob('*.gz.parquet'))
            
            if not parquet_files:
                self.logger.info(f"No CDC parquet files downloaded for {table_name}")
                return None
            
            # Read and combine all parquet files
            tables_to_combine = []
            for pq_file in parquet_files:
                table = pq.read_table(pq_file)
                tables_to_combine.append(table)
            
            combined_table = pa.concat_tables(tables_to_combine)
            
            # Write to single CDC file
            timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            cdc_file_path = table_dir / f'cdc_{timestamp_str}.parquet'
            pq.write_table(combined_table, cdc_file_path, compression='snappy')
            
            self.logger.info(f"CDC records saved to: {cdc_file_path}")
            self.logger.info(f"CDC record count: {len(combined_table):,}")
            
            # Cleanup temp files
            for pq_file in parquet_files:
                pq_file.unlink()
            local_temp_dir.rmdir()
            
            # Remove files from Snowflake stage
            cursor.execute(f"REMOVE {stage_path}")
            
            return cdc_file_path
            
        except Exception as e:
            self.logger.error(f"Failed to download CDC records for {table_name}: {str(e)}")
            raise
    
    def apply_cdc_to_table(self, table_name, cdc_file_path):
        """
        Apply CDC changes to create new current state
        
        Args:
            table_name (str): Base table name
            cdc_file_path (Path): Path to CDC parquet file
            
        Returns:
            dict: Statistics (inserts, updates, deletes, final_rows)
        """
        self.logger.info(f"Applying CDC changes to {table_name}...")
        
        try:
            # Get primary key columns
            pk_columns = self.pk_mgr.get_primary_keys(table_name)
            self.logger.info(f"Primary key columns: {pk_columns}")
            
            # Load current state
            table_dir = self.dir_manager.get_package_table_dir(self.package_name, table_name)
            current_state_path = table_dir / 'current_state.parquet'
            
            if not current_state_path.exists():
                raise FileNotFoundError(
                    f"Current state not found for {table_name}. "
                    "Please run create_base_snapshot.py first."
                )
            
            self.logger.info("Loading current state...")
            current_df = pd.read_parquet(current_state_path)
            initial_row_count = len(current_df)
            self.logger.info(f"Current state has {initial_row_count:,} rows")
            
            # Load CDC records
            self.logger.info("Loading CDC records...")
            cdc_df = pd.read_parquet(cdc_file_path)
            cdc_row_count = len(cdc_df)
            self.logger.info(f"CDC has {cdc_row_count:,} records")
            
            # Ensure column names are uppercase for consistency
            current_df.columns = current_df.columns.str.upper()
            cdc_df.columns = cdc_df.columns.str.upper()
            pk_columns = [col.upper() for col in pk_columns]
            
            # Track statistics
            insert_count = 0
            update_count = 0
            delete_count = 0
            
            # Sort CDC records by insert datetime to apply in order
            insert_dt_col = self.insert_datetime.upper()
            if insert_dt_col in cdc_df.columns:
                cdc_df = cdc_df.sort_values(insert_dt_col)
            
            # Process each CDC record
            action_col = self.metadata_action.upper()
            
            if action_col not in cdc_df.columns:
                raise ValueError(f"CDC metadata column '{self.metadata_action}' not found in CDC data")
            
            self.logger.info("Processing CDC records...")
            
            # Group CDC operations for efficiency
            inserts = cdc_df[cdc_df[action_col] == 'INSERT'].copy()
            updates = cdc_df[cdc_df[action_col] == 'UPDATE'].copy()
            deletes = cdc_df[cdc_df[action_col] == 'DELETE'].copy()
            
            # Remove CDC metadata columns from data
            metadata_cols = [col for col in cdc_df.columns if col.startswith('METADATA$')]
            data_cols = [col for col in cdc_df.columns if col not in metadata_cols and col != insert_dt_col]
            
            # Apply DELETES
            if len(deletes) > 0:
                self.logger.info(f"Processing {len(deletes)} DELETE operations...")
                
                # Create delete key tuples
                delete_keys = deletes[pk_columns].apply(tuple, axis=1).tolist()
                current_keys = current_df[pk_columns].apply(tuple, axis=1)
                
                # Remove matching rows
                mask = ~current_keys.isin(delete_keys)
                current_df = current_df[mask]
                
                delete_count = len(deletes)
                self.logger.info(f"Deleted {delete_count} rows")
            
            # Apply UPDATES
            if len(updates) > 0:
                self.logger.info(f"Processing {len(updates)} UPDATE operations...")
                
                # Set index on primary keys for efficient updates
                current_df = current_df.set_index(pk_columns)
                updates_indexed = updates[data_cols].set_index(pk_columns)
                
                # Update rows (this replaces matching rows)
                current_df.update(updates_indexed)
                current_df = current_df.reset_index()
                
                update_count = len(updates)
                self.logger.info(f"Updated {update_count} rows")
            
            # Apply INSERTS
            if len(inserts) > 0:
                self.logger.info(f"Processing {len(inserts)} INSERT operations...")
                
                # Append new rows
                inserts_data = inserts[data_cols]
                current_df = pd.concat([current_df, inserts_data], ignore_index=True)
                
                insert_count = len(inserts)
                self.logger.info(f"Inserted {insert_count} rows")
            
            final_row_count = len(current_df)
            
            # Archive current state
            timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            archive_dir = self.dir_manager.get_archive_dir(self.package_name, table_name, 'states')
            archive_path = archive_dir / f'state_{timestamp_str}.parquet'
            
            self.logger.info(f"Archiving previous current state to: {archive_path}")
            copy2(current_state_path, archive_path)
            
            # Save new current state
            self.logger.info(f"Saving new current state ({final_row_count:,} rows)...")
            current_df.to_parquet(current_state_path, compression='snappy', index=False)
            
            # Archive CDC file
            cdc_archive_dir = self.dir_manager.get_archive_dir(self.package_name, table_name, 'cdc')
            cdc_archive_path = cdc_archive_dir / f'cdc_{timestamp_str}.parquet'
            copy2(cdc_file_path, cdc_archive_path)
            
            # Remove temporary CDC file
            cdc_file_path.unlink()
            
            stats = {
                'insert_count': insert_count,
                'update_count': update_count,
                'delete_count': delete_count,
                'initial_rows': initial_row_count,
                'final_rows': final_row_count,
                'cdc_records': cdc_row_count
            }
            
            self.logger.info(f"✓ CDC applied successfully")
            self.logger.info(f"  Initial rows: {initial_row_count:,}")
            self.logger.info(f"  Inserts: {insert_count:,}")
            self.logger.info(f"  Updates: {update_count:,}")
            self.logger.info(f"  Deletes: {delete_count:,}")
            self.logger.info(f"  Final rows: {final_row_count:,}")
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to apply CDC for {table_name}: {str(e)}")
            raise
    
    def process_table(self, table_name, watermark):
        """
        Process CDC updates for a single table
        
        Args:
            table_name (str): Table name
            watermark (datetime): Watermark timestamp
            
        Returns:
            dict or None: Processing statistics
        """
        table_start_time = datetime.now()
        
        self.logger.info(f"\n{'='*80}")
        self.logger.info(f"Processing table: {table_name}")
        self.logger.info(f"{'='*80}")
        
        try:
            # Check if CDC table exists
            if not self.check_cdc_table_exists(table_name):
                self.logger.warning(f"CDC table does not exist for {table_name}. Skipping.")
                return None
            
            # Ensure directory structure
            self.dir_manager.ensure_table_structure(self.package_name, table_name)
            
            # Download CDC records
            cdc_file_path = self.download_cdc_records(table_name, watermark)
            
            if cdc_file_path is None:
                self.logger.info(f"No CDC updates for {table_name}. Skipping.")
                return None
            
            # Apply CDC changes
            stats = self.apply_cdc_to_table(table_name, cdc_file_path)
            
            # Record table metrics
            table_end_time = datetime.now()
            self.metrics_mgr.record_table_metrics(
                package_name=self.package_name,
                table_name=table_name,
                run_type='incremental_update',
                start_time=table_start_time,
                end_time=table_end_time,
                rows_processed=stats['cdc_records'],
                insert_count=stats['insert_count'],
                update_count=stats['update_count'],
                delete_count=stats['delete_count']
            )
            
            duration = (table_end_time - table_start_time).total_seconds()
            self.logger.info(f"✓ Completed {table_name} in {duration:.2f}s")
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to process {table_name}: {str(e)}")
            raise
    
    def run(self):
        """Execute CDC processing for all tables in package"""
        
        run_start_time = get_current_timestamp_est()
        
        self.logger.info("="*80)
        self.logger.info(f"Starting CDC Update Processing for Package: {self.package_name}")
        self.logger.info(f"Timestamp: {run_start_time}")
        self.logger.info("="*80)
        
        try:
            # Get watermark
            self.logger.info("Retrieving package watermark...")
            watermark = self.watermark_mgr.get_watermark(self.package_name)
            
            if watermark is None:
                self.logger.error(f"No watermark found for package '{self.package_name}'")
                self.logger.error("Please run create_base_snapshot.py first.")
                return 1
            
            self.logger.info(f"Package Watermark: {watermark}")
            
            # Connect to Snowflake
            self.logger.info("Connecting to Snowflake...")
            self.conn = self.sf_connector.connect()
            self.logger.info("✓ Connected to Snowflake")
            
            # Get tables for package
            tables = self.get_package_tables()
            
            if not tables:
                self.logger.error("No tables found for package. Exiting.")
                return 1
            
            # Process each table
            total_rows = 0
            total_inserts = 0
            total_updates = 0
            total_deletes = 0
            successful_tables = 0
            skipped_tables = 0
            failed_tables = []
            
            for idx, table_name in enumerate(tables, 1):
                self.logger.info(f"\n[{idx}/{len(tables)}] Processing: {table_name}")
                
                try:
                    stats = self.process_table(table_name, watermark)
                    
                    if stats is None:
                        skipped_tables += 1
                    else:
                        successful_tables += 1
                        total_rows += stats['cdc_records']
                        total_inserts += stats['insert_count']
                        total_updates += stats['update_count']
                        total_deletes += stats['delete_count']
                    
                except Exception as e:
                    self.logger.error(f"Failed to process {table_name}: {str(e)}")
                    failed_tables.append(table_name)
            
            # If any failures, rollback would go here
            # For now, we'll just report the failures
            if failed_tables:
                self.logger.error(f"\n{'='*80}")
                self.logger.error("PROCESSING FAILED FOR SOME TABLES")
                self.logger.error(f"{'='*80}")
                self.logger.error(f"Failed tables: {', '.join(failed_tables)}")
                self.logger.error("Manual intervention may be required.")
                self.logger.error(f"{'='*80}")
                
                run_end_time = get_current_timestamp_est()
                
                # Record failed run
                self.metrics_mgr.record_run(
                    package_name=self.package_name,
                    run_type='incremental_update',
                    start_time=run_start_time,
                    end_time=run_end_time,
                    tables_processed=successful_tables,
                    total_rows=total_rows,
                    status='failed',
                    error_message=f"Failed tables: {', '.join(failed_tables)}"
                )
                
                return 1
            
            # Update watermark
            run_end_time = get_current_timestamp_est()
            
            self.watermark_mgr.set_watermark(
                package_name=self.package_name,
                timestamp=run_end_time,
                run_type='incremental_update',
                status='success',
                tables_count=successful_tables
            )
            
            # Record run metrics
            self.metrics_mgr.record_run(
                package_name=self.package_name,
                run_type='incremental_update',
                start_time=run_start_time,
                end_time=run_end_time,
                tables_processed=successful_tables,
                total_rows=total_rows,
                status='success',
                error_message=None
            )
            
            # Summary
            duration = (run_end_time - run_start_time).total_seconds()
            
            self.logger.info("\n" + "="*80)
            self.logger.info("CDC UPDATE PROCESSING SUMMARY")
            self.logger.info("="*80)
            self.logger.info(f"Package: {self.package_name}")
            self.logger.info(f"Total Tables: {len(tables)}")
            self.logger.info(f"Processed: {successful_tables}")
            self.logger.info(f"Skipped (no updates): {skipped_tables}")
            self.logger.info(f"Failed: {len(failed_tables)}")
            self.logger.info(f"Total CDC Records: {total_rows:,}")
            self.logger.info(f"  Inserts: {total_inserts:,}")
            self.logger.info(f"  Updates: {total_updates:,}")
            self.logger.info(f"  Deletes: {total_deletes:,}")
            self.logger.info(f"Duration: {duration:.2f}s")
            self.logger.info(f"New Watermark: {run_end_time}")
            self.logger.info("="*80)
            
            return 0
            
        except Exception as e:
            run_end_time = get_current_timestamp_est()
            
            self.logger.error(f"CDC processing failed: {str(e)}")
            
            # Record failed run
            self.metrics_mgr.record_run(
                package_name=self.package_name,
                run_type='incremental_update',
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
        description='Apply CDC updates for a package'
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
    
    # Create and run CDC processor
    processor = CDCProcessor(args.config, args.package)
    return processor.run()


if __name__ == "__main__":
    sys.exit(main())
