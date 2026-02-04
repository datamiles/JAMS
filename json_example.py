"""
Diagnostic Script - Check CDC Tables
"""

import yaml
import snowflake.connector

# Load config
with open('config.yaml') as f:
    config = yaml.safe_load(f)

# Connect to Snowflake
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    account=config['snowflake']['account'],
    user=config['snowflake']['user'],
    password=config['snowflake']['password'],
    warehouse=config['snowflake']['warehouse'],
    database=config['snowflake']['database'],
    schema=config['snowflake']['schema'],
    role=config['snowflake'].get('role')
)

print("Connected!\n")

# Get CDC schema from config - should come from cdc section
cdc_schema = config['cdc'].get('cdc_schema')

# If not in cdc section, fall back to snowflake schema
if not cdc_schema:
    cdc_schema = config['snowflake']['schema']
    print("WARNING: cdc_schema not found in cdc section, using snowflake schema as fallback")

database = config['snowflake']['database']

print("=" * 80)
print("CDC TABLE DIAGNOSTICS")
print("=" * 80)
print(f"Database: {database}")
print(f"CDC Schema: {cdc_schema}")
print(f"CDC Prefix: {config['cdc'].get('cdc_prefix', 'Not set')}")
print(f"CDC Suffix: {config['cdc'].get('cdc_suffix', 'Not set')}")
print()

# Check if CDC schema exists
print("Checking if CDC schema exists...")
cursor = conn.cursor()
cursor.execute(f"""
    SELECT COUNT(*) 
    FROM INFORMATION_SCHEMA.SCHEMATA 
    WHERE SCHEMA_NAME = '{cdc_schema}'
    AND CATALOG_NAME = '{database}'
""")
schema_exists = cursor.fetchone()[0]

if schema_exists:
    print(f"✓ CDC schema '{cdc_schema}' exists in database '{database}'")
else:
    print(f"✗ CDC schema '{cdc_schema}' DOES NOT EXIST in database '{database}'")
    print("\nAvailable schemas:")
    cursor.execute(f"SHOW SCHEMAS IN DATABASE {database}")
    for row in cursor.fetchall():
        print(f"  - {row[1]}")  # Schema name is in second column
    print()
    conn.close()
    exit(1)

print()

# Get all tables in CDC schema
print(f"Checking tables in {database}.{cdc_schema}...")
cursor.execute(f"""
    SELECT TABLE_NAME 
    FROM INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_SCHEMA = '{cdc_schema}'
    AND TABLE_CATALOG = '{database}'
    ORDER BY TABLE_NAME
""")

cdc_tables = [row[0] for row in cursor.fetchall()]

print(f"Found {len(cdc_tables)} tables in {cdc_schema}:")
print("-" * 80)

if cdc_tables:
    for table in cdc_tables:
        print(f"  {table}")
else:
    print("  (No tables found)")

print("-" * 80)
print()

# Now check base tables from package
package_name = input("Enter package name to check (e.g., 'Market Splits'): ").strip()

if package_name:
    loadtracker_table = config['snowflake']['loadtracker_table']
    
    print(f"\nGetting tables for package '{package_name}' from loadtracker...")
    cursor.execute(f"""
        SELECT DISTINCT TABLENAME 
        FROM {loadtracker_table} 
        WHERE PACKAGENAME = %s
        ORDER BY TABLENAME
    """, (package_name,))
    
    base_tables = [row[0] for row in cursor.fetchall()]
    
    if not base_tables:
        print(f"✗ No tables found for package '{package_name}'")
        conn.close()
        exit(1)
    
    print(f"Found {len(base_tables)} base tables in package '{package_name}'")
    print()
    
    # Check CDC table mapping
    print("=" * 80)
    print("CDC TABLE MAPPING CHECK")
    print("=" * 80)
    
    cdc_prefix = config['cdc'].get('cdc_prefix', '')
    cdc_suffix = config['cdc'].get('cdc_suffix', '')
    
    print(f"Using CDC prefix: '{cdc_prefix}'")
    print(f"Using CDC suffix: '{cdc_suffix}'")
    print()
    
    found = 0
    missing = 0
    
    for base_table in base_tables[:10]:  # Check first 10 tables
        # Construct expected CDC table name
        if cdc_prefix:
            expected_cdc = f"{cdc_prefix}{base_table}"
        else:
            expected_cdc = f"{base_table}{cdc_suffix}"
        
        # Convert to UPPERCASE (Snowflake default)
        expected_cdc = expected_cdc.upper()
        
        # Check if it exists (CDC tables list is already uppercase)
        exists = expected_cdc in cdc_tables
        
        status = "✓ EXISTS" if exists else "✗ MISSING"
        print(f"{base_table:40} -> {expected_cdc:40} {status}")
        
        if exists:
            found += 1
        else:
            missing += 1
    
    if len(base_tables) > 10:
        print(f"... and {len(base_tables) - 10} more tables")
    
    print("-" * 80)
    print(f"Summary: {found} found, {missing} missing (out of {min(len(base_tables), 10)} checked)")
    print()
    
    # Suggest corrections
    if missing > 0:
        print("=" * 80)
        print("SUGGESTIONS")
        print("=" * 80)
        
        # Try to detect pattern
        if cdc_tables:
            sample_cdc = cdc_tables[0]
            sample_base = base_tables[0] if base_tables else "TABLE"
            
            print("Sample CDC table found:", sample_cdc)
            print("Sample base table:", sample_base)
            print()
            
            # Check if prefix exists
            if sample_cdc.startswith("CDC_"):
                print("Looks like CDC tables use 'CDC_' prefix")
                print("Your config should have:")
                print("  cdc_prefix: 'CDC_'")
                print("  cdc_suffix: ''")
            elif sample_cdc.endswith("_CDC"):
                print("Looks like CDC tables use '_CDC' suffix")
                print("Your config should have:")
                print("  cdc_prefix: ''")
                print("  cdc_suffix: '_CDC'")
            else:
                print("Cannot determine CDC naming pattern.")
                print("Please check the actual CDC table names manually.")
            print()
            
            # Check schema
            print("Also verify:")
            print(f"  cdc_schema: '{cdc_schema}' (currently configured)")
            print()

conn.close()
print("Done!")
