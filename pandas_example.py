"""
Script: Apply CDC Updates (Simplified - Direct Query Method)
Downloads CDC records directly to DataFrames and applies them to create new current state
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


class CDCProcessorSimplified:
    """Process CDC updates using direct DataFrame approach"""
    
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
        self.cdc_schema = self.config['cdc'].get('cdc_schema', self.config['snowflake']['schema'])
        self.metadata_action = self.config['cdc']['metadata_action']
        self.metadata_isupdate = self.config['cdc']['metadata_isupdate']
        self.insert_datetime = self.config['cdc']['insert_datetime']
    
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
    
    def check_cdc_table_exists(self, table_name):
        """Check if CDC table exists for the given base table"""
        # Build CDC table name
        if self.cdc_prefix:
            cdc_table_name = f"{self.cdc_prefix}{table_name}"
        else:
            cdc_table_name = f"{table_name}{self.cdc_suffix}"
        
        cdc_table_name = cdc_table_name.upper()
        cdc_schema = self.cdc_schema.upper()
        
        query = """
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
        """
        
        cursor = self.conn.cursor()
        cursor.execute(query, (cdc_schema, cdc_table_name))
        count = cursor.fetchone()[0]
        
        return count > 0
    
    def query_cdc_records(self, table_name, watermark):
        """
        Query CDC records directly into DataFrame
        
        Args:
            table_name (str): Base table name
            watermark (datetime): Watermark timestamp
            
        Returns:
            DataFrame or None: CDC records
        """
        # Build CDC table name
        if self.cdc_prefix:
            cdc_table_name = f"{self.cdc_prefix}{table_name}"
        else:
            cdc_table_name = f"{table_name}{self.cdc_suffix}"
        
        cdc_table_name = cdc_table_name.upper()
        cdc_schema = self.cdc_schema.upper()
        database = self.config['snowflake']['database'].upper()
        
        # Fully qualified table name with quotes
        qualified_cdc_table = f'"{database}"."{cdc_schema}"."{cdc_table_name}"'
        
        self.logger.info(f"Querying CDC records for {table_name}...")
        self.logger.info(f"CDC table: {qualified_cdc_table}")
        self.logger.info(f"Since watermark: {watermark}")
        
        # Query CDC records
        query = f"""
        SELECT *
        FROM {qualified_cdc_table}
        WHERE "{self.insert_datetime}" > %s
        ORDER BY "{self.insert_datetime}"
        """
        
        cursor = self.conn.cursor()
        cursor.execute(query, (watermark,))
        
        # Fetch data
        rows = cursor.fetchall()
        
        if not rows:
            self.logger.info(f"No CDC records found for {table_name}")
            return None
        
        # Get column names and create DataFrame
        columns = [desc[0] for desc in cursor.description]
        cdc_df = pd.DataFrame(rows, columns=columns)
        
        self.logger.info(f"Retrieved {len(cdc_df):,} CDC records")
        
        return cdc_df
    
    def save_cdc_records_archive(self, table_name, cdc_df):
        """Save CDC records to archive for reference"""
        # Get archives directory
        archives_dir = self.dir_manager.get_archives_dir()
        cdc_archive_dir = archives_dir / self.package_name_safe / table_name / 'cdc'
        cdc_archive_dir.mkdir(parents=True, exist_ok=True)
        
        # Create timestamped filename
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        cdc_archive_path = cdc_archive_dir / f'cdc_{timestamp_str}.parquet'
        
        # Save CDC records
        cdc_df.to_parquet(cdc_archive_path, compression='snappy', index=False)
        
        self.logger.info(f"Archived CDC records: {cdc_archive_path}")
        
        return cdc_archive_path
    
    def archive_current_state(self, table_name):
        """Archive current state before applying CDC"""
        table_dir = self.dir_manager.get_package_table_dir(self.package_name_safe, table_name)
        current_state_path = table_dir / 'current_state.parquet'
        
        if not current_state_path.exists():
            self.logger.warning(f"No current state found to archive for {table_name}")
            return None
        
        # Get archives directory
        archives_dir = self.dir_manager.get_archives_dir()
        state_archive_dir = archives_dir / self.package_name_safe / table_name / 'states'
        state_archive_dir.mkdir(parents=True, exist_ok=True)
        
        # Create timestamped filename
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        state_archive_path = state_archive_dir / f'state_{timestamp_str}.parquet'
        
        # Copy current state to archive
        copy2(current_state_path, state_archive_path)
        
        self.logger.info(f"Archived previous state: {state_archive_path}")
        
        return state_archive_path
    
    def apply_cdc_changes(self, table_name, cdc_df):
        """
        Apply CDC changes to current state
        
        Args:
            table_name (str): Base table name
            cdc_df (DataFrame): CDC records
            
        Returns:
            dict: Statistics about changes applied
        """
        self.logger.info(f"Applying CDC changes to {table_name}...")
        
        # Get primary key columns
        pk_columns = self.pk_mgr.get_primary_keys(table_name)
        self.logger.info(f"Primary key columns: {pk_columns}")
        
        # Load current state
        table_dir = self.dir_manager.get_package_table_dir(self.package_name_safe, table_name)
        current_state_path = table_dir / 'current_state.parquet'
        
        if not current_state_path.exists():
            raise FileNotFoundError(
                f"Current state not found for {table_name}. "
                "Please run create_base_snapshot.py first."
            )
        
        current_df = pd.read_parquet(current_state_path)
        initial_row_count = len(current_df)
        self.logger.info(f"Current state has {initial_row_count:,} rows")
        
        # Ensure column names are uppercase for consistency
        current_df.columns = current_df.columns.str.upper()
        cdc_df.columns = cdc_df.columns.str.upper()
        pk_columns = [col.upper() for col in pk_columns]
        
        # Track statistics
        insert_count = 0
        update_count = 0
        delete_count = 0
        
        # Sort CDC records by insert datetime
        insert_dt_col = self.insert_datetime.upper()
        if insert_dt_col in cdc_df.columns:
            cdc_df = cdc_df.sort_values(insert_dt_col)
        
        # Get action column
        action_col = self.metadata_action.upper()
        
        if action_col not in cdc_df.columns:
            raise ValueError(f"CDC metadata column '{self.metadata_action}' not found")
        
        # Group CDC operations
        inserts = cdc_df[cdc_df[action_col] == 'INSERT'].copy()
        updates = cdc_df[cdc_df[action_col] == 'UPDATE'].copy()
        deletes = cdc_df[cdc_df[action_col] == 'DELETE'].copy()
        
        self.logger.info(f"CDC operations: {len(inserts)} INSERTs, {len(updates)} UPDATEs, {len(deletes)} DELETEs")
        
        # Remove CDC metadata columns
        metadata_cols = [col for col in cdc_df.columns if col.startswith('METADATA$')]
        data_cols = [col for col in cdc_df.columns if col not in metadata_cols and col != insert_dt_col]
        
        # Apply DELETES
        if len(deletes) > 0:
            self.logger.info(f"Processing {len(deletes)} DELETE operations...")
            delete_keys = deletes[pk_columns].apply(tuple, axis=1).tolist()
            current_keys = current_df[pk_columns].apply(tuple, axis=1)
            mask = ~current_keys.isin(delete_keys)
            current_df = current_df[mask]
            delete_count = len(deletes)
            self.logger.info(f"Deleted {delete_count} rows")
        
        # Apply UPDATES
        if len(updates) > 0:
            self.logger.info(f"Processing {len(updates)} UPDATE operations...")
            update_keys = updates[pk_columns].apply(tuple, axis=1).tolist()
            current_keys = current_df[pk_columns].apply(tuple, axis=1)
            mask = ~current_keys.isin(update_keys)
            current_df = current_df[mask]
            updates_clean = updates[data_cols].copy()
            current_df = pd.concat([current_df, updates_clean], ignore_index=True)
            update_count = len(updates)
            self.logger.info(f"Updated {update_count} rows")
        
        # Apply INSERTS
        if len(inserts) > 0:
            self.logger.info(f"Processing {len(inserts)} INSERT operations...")
            inserts_clean = inserts[data_cols].copy()
            current_df = pd.concat([current_df, inserts_clean], ignore_index=True)
            insert_count = len(inserts)
            self.logger.info(f"Inserted {insert_count} rows")
        
        final_row_count = len(current_df)
        self.logger.info(f"New state has {final_row_count:,} rows (was {initial_row_count:,})")
        
        # Save new current state
        current_df.to_parquet(current_state_path, compression='snappy', index=False)
        self.logger.info(f"Saved new current state: {current_state_path}")
        
        return {
            'initial_rows': initial_row_count,
            'final_rows': final_row_count,
            'insert_count': insert_count,
            'update_count': update_count,
            'delete_count': delete_count,
            'total_cdc': len(cdc_df)
        }
    
    def process_table(self, table_name, watermark):
        """Process CDC updates for a single table"""
        table_start_time = datetime.now()
        
        self.logger.info("="*80)
        self.logger.info(f"Processing table: {table_name}")
        self.logger.info("="*80)
        
        try:
            # Check if CDC table exists
            if not self.check_cdc_table_exists(table_name):
                self.logger.warning(f"CDC table does not exist for {table_name}. Skipping.")
                return None
            
            self.logger.info(f"CDC table exists for {table_name}")
            
            # Query CDC records
            cdc_df = self.query_cdc_records(table_name, watermark)
            
            if cdc_df is None or len(cdc_df) == 0:
                self.logger.info(f"No CDC records to process for {table_name}")
                return None
            
            # Archive CDC records for reference
            self.save_cdc_records_archive(table_name, cdc_df)
            
            # Archive current state before changes
            self.archive_current_state(table_name)
            
            # Apply CDC changes
            stats = self.apply_cdc_changes(table_name, cdc_df)
            
            # Record metrics
            table_end_time = datetime.now()
            self.metrics_mgr.record_table_metrics(
                package_name=self.package_name_safe,
                table_name=table_name,
                run_type='cdc_update',
                start_time=table_start_time,
                end_time=table_end_time,
                rows_processed=stats['total_cdc'],
                insert_count=stats['insert_count'],
                update_count=stats['update_count'],
                delete_count=stats['delete_count']
            )
            
            duration = (table_end_time - table_start_time).total_seconds()
            self.logger.info(f"Completed {table_name} in {duration:.2f}s")
            self.logger.info(f"  Inserts: {stats['insert_count']}, Updates: {stats['update_count']}, Deletes: {stats['delete_count']}")
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to process {table_name}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise
    
    def run(self):
        """Execute CDC processing for all tables in package"""
        run_start_time = get_current_timestamp_est()
        
        self.logger.info("="*80)
        self.logger.info(f"Starting CDC Update Application")
        self.logger.info(f"Package: {self.package_name}")
        self.logger.info(f"Timestamp: {run_start_time}")
        self.logger.info("="*80)
        
        try:
            # Get watermark
            self.logger.info("Retrieving package watermark...")
            watermark = self.watermark_mgr.get_watermark(self.package_name_safe)
            
            if watermark is None:
                self.logger.error(f"No watermark found for package '{self.package_name}'")
                self.logger.error("Please run create_base_snapshot.py first")
                return 2
            
            self.logger.info(f"Package watermark: {watermark}")
            
            # Connect to Snowflake
            self.logger.info("Connecting to Snowflake...")
            self.conn = self.sf_connector.connect()
            self.logger.info("Connected to Snowflake successfully")
            
            # Get tables
            tables = self.get_package_tables()
            
            if not tables:
                self.logger.error("No tables found for package")
                return 1
            
            # Process each table
            total_cdc_records = 0
            successful_tables = 0
            failed_tables = []
            tables_with_updates = []
            
            for idx, table_name in enumerate(tables, 1):
                self.logger.info(f"\nProcessing table {idx}/{len(tables)}: {table_name}")
                
                try:
                    stats = self.process_table(table_name, watermark)
                    
                    if stats:
                        total_cdc_records += stats['total_cdc']
                        successful_tables += 1
                        tables_with_updates.append(table_name)
                    else:
                        self.logger.info(f"No updates for {table_name}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to process {table_name}: {str(e)}")
                    failed_tables.append(table_name)
            
            # Update watermark
            if tables_with_updates:
                run_end_time = get_current_timestamp_est()
                self.watermark_mgr.set_watermark(
                    package_name=self.package_name_safe,
                    timestamp=run_end_time,
                    run_type='cdc_update',
                    status='success' if not failed_tables else 'partial',
                    tables_count=len(tables_with_updates)
                )
                self.logger.info(f"Updated watermark to: {run_end_time}")
            else:
                self.logger.info("No tables had updates, watermark not changed")
                run_end_time = get_current_timestamp_est()
            
            # Record run metrics
            self.metrics_mgr.record_run(
                package_name=self.package_name_safe,
                run_type='cdc_update',
                start_time=run_start_time,
                end_time=run_end_time,
                tables_processed=successful_tables,
                total_rows=total_cdc_records,
                status='success' if not failed_tables else 'partial',
                error_message=f"Failed: {', '.join(failed_tables)}" if failed_tables else None
            )
            
            # Summary
            duration = (run_end_time - run_start_time).total_seconds()
            
            self.logger.info("\n" + "="*80)
            self.logger.info("CDC UPDATE SUMMARY")
            self.logger.info("="*80)
            self.logger.info(f"Package: {self.package_name}")
            self.logger.info(f"Total Tables: {len(tables)}")
            self.logger.info(f"Tables with Updates: {len(tables_with_updates)}")
            self.logger.info(f"Successful: {successful_tables}")
            self.logger.info(f"Failed: {len(failed_tables)}")
            self.logger.info(f"Total CDC Records: {total_cdc_records:,}")
            self.logger.info(f"Duration: {duration:.2f}s")
            
            if failed_tables:
                self.logger.warning(f"Failed tables: {', '.join(failed_tables)}")
            
            self.logger.info("="*80)
            
            return 0 if not failed_tables else 1
            
        except Exception as e:
            run_end_time = get_current_timestamp_est()
            self.logger.error(f"CDC processing failed: {str(e)}")
            
            self.metrics_mgr.record_run(
                package_name=self.package_name_safe,
                run_type='cdc_update',
                start_time=run_start_time,
                end_time=run_end_time,
                tables_processed=0,
                total_rows=0,
                status='failed',
                error_message=str(e)
            )
            
            return 1
            
        finally:
            if self.conn:
                self.sf_connector.close()
                self.logger.info("Snowflake connection closed")


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description='Apply CDC updates to package (Simplified Direct Query Method)'
    )
    parser.add_argument('--package', required=True, help='Package name')
    parser.add_argument('--config', default='config.yaml', help='Config file')
    
    args = parser.parse_args()
    
    processor = CDCProcessorSimplified(args.config, args.package)
    return processor.run()


if __name__ == "__main__":
    sys.exit(main())
