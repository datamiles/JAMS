"""
List Available Packages from Loadtracker
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

print("Connected successfully!")
print()

# Query loadtracker for packages
loadtracker = config['snowflake']['loadtracker_table']

# Simple query without conversion issues
query = f"""
SELECT 
    PACKAGENAME,
    COUNT(DISTINCT TABLENAME) as TABLE_COUNT
FROM {loadtracker}
GROUP BY PACKAGENAME
ORDER BY PACKAGENAME
"""

cursor = conn.cursor()
cursor.execute(query)

print("=" * 80)
print("AVAILABLE PACKAGES IN LOADTRACKER")
print("=" * 80)
print()
print(f"{'Package Name':<40} {'Tables':<10}")
print("-" * 80)

packages = []
try:
    for row in cursor.fetchall():
        package_name = row[0]
        table_count = row[1]
        packages.append(package_name)
        print(f"{package_name:<40} {table_count:<10}")
except Exception as e:
    print(f"Error fetching rows: {e}")
    print("Trying alternative query...")
    
    # Fallback: simpler query
    cursor.execute(f"SELECT DISTINCT PACKAGENAME FROM {loadtracker} ORDER BY PACKAGENAME")
    packages = [row[0] for row in cursor.fetchall()]
    for pkg in packages:
        print(f"{pkg:<40}")

print("-" * 80)
print(f"Total packages: {len(packages)}")
print()

if packages:
    print("To create base snapshot for a package, run:")
    print(f"  python create_base_snapshot.py --package {packages[0]} --config config.yaml")
    print()
    print("Available packages:")
    for pkg in packages:
        print(f"  - {pkg}")
else:
    print("WARNING: No packages found in loadtracker!")

conn.close()
