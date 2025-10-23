"""
Script to read Parquet file using DuckDB and load data into SQL Server table.

Requirements:
    pip install duckdb pyodbc pandas --break-system-packages

Usage:
    python parquet_to_sqlserver.py
"""

import duckdb
import pyodbc
import pandas as pd
from typing import Optional


class ParquetToSQLServer:
    def __init__(self, server: str, database: str, username: Optional[str] = None, 
                 password: Optional[str] = None, trusted_connection: bool = False):
        """
        Initialize the ParquetToSQLServer class.
        
        Args:
            server: SQL Server instance name or IP
            database: Database name
            username: SQL Server username (if using SQL authentication)
            password: SQL Server password (if using SQL authentication)
            trusted_connection: Use Windows authentication if True
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.trusted_connection = trusted_connection
        
    def get_connection_string(self) -> str:
        """Build SQL Server connection string."""
        driver = '{ODBC Driver 17 for SQL Server}'
        
        if self.trusted_connection:
            conn_str = (
                f'DRIVER={driver};'
                f'SERVER={self.server};'
                f'DATABASE={self.database};'
                f'Trusted_Connection=yes;'
            )
        else:
            conn_str = (
                f'DRIVER={driver};'
                f'SERVER={self.server};'
                f'DATABASE={self.database};'
                f'UID={self.username};'
                f'PWD={self.password};'
            )
        
        return conn_str
    
    def read_parquet_with_duckdb(self, parquet_file: str) -> pd.DataFrame:
        """
        Read Parquet file using DuckDB.
        
        Args:
            parquet_file: Path to the Parquet file
            
        Returns:
            pandas DataFrame containing the data
        """
        print(f"Reading Parquet file: {parquet_file}")
        
        # Create DuckDB connection
        conn = duckdb.connect(':memory:')
        
        # Read Parquet file into a DataFrame
        query = f"SELECT * FROM read_parquet('{parquet_file}')"
        df = conn.execute(query).fetchdf()
        
        conn.close()
        
        print(f"Successfully read {len(df)} rows from Parquet file")
        return df
    
    def load_to_sqlserver(self, df: pd.DataFrame, table_name: str, 
                         if_exists: str = 'append', batch_size: int = 1000):
        """
        Load DataFrame into SQL Server table.
        
        Args:
            df: pandas DataFrame to load
            table_name: Target SQL Server table name
            if_exists: What to do if table exists ('fail', 'replace', 'append')
            batch_size: Number of rows to insert per batch
        """
        print(f"Loading data to SQL Server table: {table_name}")
        
        conn_str = self.get_connection_string()
        
        try:
            # Connect to SQL Server
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # Handle table existence
            if if_exists == 'replace':
                cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}")
                conn.commit()
            
            # Create table if it doesn't exist
            if if_exists in ['replace', 'fail']:
                create_table_sql = self._generate_create_table_sql(df, table_name)
                cursor.execute(create_table_sql)
                conn.commit()
                print(f"Created table: {table_name}")
            
            # Insert data in batches
            columns = list(df.columns)
            placeholders = ', '.join(['?' for _ in columns])
            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            total_rows = len(df)
            for i in range(0, total_rows, batch_size):
                batch = df.iloc[i:i + batch_size]
                data_to_insert = [tuple(row) for row in batch.values]
                cursor.executemany(insert_sql, data_to_insert)
                conn.commit()
                print(f"Inserted rows {i + 1} to {min(i + batch_size, total_rows)} of {total_rows}")
            
            print(f"Successfully loaded {total_rows} rows into {table_name}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"Error loading data to SQL Server: {e}")
            raise
    
    def _generate_create_table_sql(self, df: pd.DataFrame, table_name: str) -> str:
        """
        Generate CREATE TABLE SQL statement based on DataFrame dtypes.
        
        Args:
            df: pandas DataFrame
            table_name: Table name
            
        Returns:
            CREATE TABLE SQL statement
        """
        type_mapping = {
            'int64': 'BIGINT',
            'int32': 'INT',
            'float64': 'FLOAT',
            'float32': 'REAL',
            'object': 'NVARCHAR(MAX)',
            'bool': 'BIT',
            'datetime64[ns]': 'DATETIME2',
            'timedelta64[ns]': 'BIGINT'
        }
        
        columns_sql = []
        for col, dtype in df.dtypes.items():
            sql_type = type_mapping.get(str(dtype), 'NVARCHAR(MAX)')
            columns_sql.append(f"[{col}] {sql_type}")
        
        create_sql = f"CREATE TABLE {table_name} (\n    " + ',\n    '.join(columns_sql) + "\n)"
        return create_sql
    
    def process(self, parquet_file: str, table_name: str, 
                if_exists: str = 'append', batch_size: int = 1000):
        """
        Main method to read Parquet and load to SQL Server.
        
        Args:
            parquet_file: Path to Parquet file
            table_name: SQL Server table name
            if_exists: What to do if table exists ('fail', 'replace', 'append')
            batch_size: Number of rows to insert per batch
        """
        # Read Parquet file
        df = self.read_parquet_with_duckdb(parquet_file)
        
        # Load to SQL Server
        self.load_to_sqlserver(df, table_name, if_exists, batch_size)


def main():
    """Example usage of the ParquetToSQLServer class."""
    
    # Configuration - Update these values for your environment
    SERVER = 'localhost'  # or '192.168.1.100' or 'server.domain.com'
    DATABASE = 'TestDB'
    USERNAME = 'your_username'  # Only needed if not using Windows auth
    PASSWORD = 'your_password'  # Only needed if not using Windows auth
    TRUSTED_CONNECTION = True  # Set to True for Windows authentication
    
    PARQUET_FILE = 'data.parquet'  # Path to your Parquet file
    TABLE_NAME = 'dbo.YourTableName'  # Target table name
    
    # Initialize the loader
    loader = ParquetToSQLServer(
        server=SERVER,
        database=DATABASE,
        username=USERNAME,
        password=PASSWORD,
        trusted_connection=TRUSTED_CONNECTION
    )
    
    # Process the file
    # if_exists options: 'fail' (error if exists), 'replace' (drop and recreate), 'append' (add to existing)
    loader.process(
        parquet_file=PARQUET_FILE,
        table_name=TABLE_NAME,
        if_exists='append',
        batch_size=1000
    )


if __name__ == '__main__':
    main()
