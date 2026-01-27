"""
SQL Server Primary Key Extractor
Extracts primary key information from SQL Server and updates pk_mappings.json
"""

import pyodbc
import json
import yaml
from datetime import datetime
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SQLServerPKExtractor:
    def __init__(self, config_path='config.yaml'):
        """Initialize the PK extractor with configuration"""
        self.config = self._load_config(config_path)
        self.conn = None
        
    def _load_config(self, config_path):
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def connect(self):
        """Establish connection to SQL Server"""
        try:
            sqlserver_config = self.config['sqlserver']
            
            # Determine authentication type
            auth_type = sqlserver_config.get('auth_type', 'sql').lower()
            
            if auth_type == 'windows':
                # Windows Authentication
                conn_string = (
                    f"DRIVER={{{sqlserver_config['driver']}}};"
                    f"SERVER={sqlserver_config['server']};"
                    f"DATABASE={sqlserver_config['database']};"
                    f"Trusted_Connection=yes;"
                )
                logger.info("Using Windows Authentication for SQL Server")
            else:
                # SQL Server Authentication
                conn_string = (
                    f"DRIVER={{{sqlserver_config['driver']}}};"
                    f"SERVER={sqlserver_config['server']};"
                    f"DATABASE={sqlserver_config['database']};"
                    f"UID={sqlserver_config['username']};"
                    f"PWD={sqlserver_config['password']}"
                )
                logger.info("Using SQL Server Authentication")
            
            self.conn = pyodbc.connect(conn_string)
            logger.info("Successfully connected to SQL Server")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {str(e)}")
            raise
    
    def extract_primary_keys(self, schema=None):
        """
        Extract primary key information for all tables in the specified schema
        
        Returns:
            dict: Dictionary with table names as keys and primary key columns as values
        """
        if schema is None:
            schema = self.config['sqlserver']['schema']
        
        query = """
        SELECT 
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            c.COLUMN_NAME,
            c.ORDINAL_POSITION
        FROM 
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        INNER JOIN 
            INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE c 
            ON tc.CONSTRAINT_NAME = c.CONSTRAINT_NAME
        INNER JOIN 
            INFORMATION_SCHEMA.TABLES t 
            ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE 
            tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            AND t.TABLE_SCHEMA = ?
        ORDER BY 
            t.TABLE_NAME, c.ORDINAL_POSITION
        """
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (schema,))
            
            pk_mappings = {}
            
            for row in cursor.fetchall():
                table_schema, table_name, column_name, ordinal = row
                
                if table_name not in pk_mappings:
                    pk_mappings[table_name] = []
                
                pk_mappings[table_name].append(column_name)
            
            logger.info(f"Extracted primary keys for {len(pk_mappings)} tables")
            return pk_mappings
            
        except Exception as e:
            logger.error(f"Failed to extract primary keys: {str(e)}")
            raise
    
    def update_pk_mappings_file(self, pk_mappings, output_path='pk_mappings.json'):
        """
        Update or create pk_mappings.json file with extracted primary keys
        
        Args:
            pk_mappings (dict): Dictionary of table names to primary key columns
            output_path (str): Path to output JSON file
        """
        try:
            # Load existing file if it exists
            pk_file = Path(output_path)
            if pk_file.exists():
                with open(pk_file, 'r') as f:
                    existing_data = json.load(f)
                    logger.info(f"Loaded existing pk_mappings.json with {len(existing_data.get('tables', {}))} tables")
            else:
                existing_data = {
                    "description": "Primary Key Mappings for Tables",
                    "tables": {}
                }
                logger.info("Creating new pk_mappings.json file")
            
            # Update with new mappings
            for table_name, pk_columns in pk_mappings.items():
                existing_data['tables'][table_name] = {
                    "primary_keys": pk_columns,
                    "description": f"Auto-extracted on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                }
            
            # Update last_updated timestamp
            existing_data['last_updated'] = datetime.now().isoformat()
            
            # Write back to file
            with open(output_path, 'w') as f:
                json.dump(existing_data, f, indent=2)
            
            logger.info(f"Successfully updated {output_path} with {len(pk_mappings)} tables")
            logger.info(f"Total tables in pk_mappings.json: {len(existing_data['tables'])}")
            
        except Exception as e:
            logger.error(f"Failed to update pk_mappings.json: {str(e)}")
            raise
    
    def close(self):
        """Close SQL Server connection"""
        if self.conn:
            self.conn.close()
            logger.info("SQL Server connection closed")


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Extract primary keys from SQL Server and update pk_mappings.json'
    )
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='Path to configuration file (default: config.yaml)'
    )
    parser.add_argument(
        '--output',
        default='pk_mappings.json',
        help='Path to output pk_mappings.json file (default: pk_mappings.json)'
    )
    parser.add_argument(
        '--schema',
        help='SQL Server schema to extract from (default: from config.yaml)'
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize extractor
        extractor = SQLServerPKExtractor(args.config)
        
        # Connect to SQL Server
        extractor.connect()
        
        # Extract primary keys
        pk_mappings = extractor.extract_primary_keys(schema=args.schema)
        
        # Display extracted information
        print("\n" + "="*60)
        print("EXTRACTED PRIMARY KEYS")
        print("="*60)
        for table, columns in sorted(pk_mappings.items()):
            print(f"{table:30} -> {', '.join(columns)}")
        print("="*60 + "\n")
        
        # Update pk_mappings.json
        extractor.update_pk_mappings_file(pk_mappings, args.output)
        
        print(f"✓ Successfully updated {args.output}")
        
        # Close connection
        extractor.close()
        
    except Exception as e:
        logger.error(f"Execution failed: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
