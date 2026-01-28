"""
List Available Packages from Loadtracker
"""

import yaml
import snowflake.connector

# Load config
with open('config.yaml') as f:
    config = yaml.safe_load(f)

# Connect to Snowflake
conn = snowflake.connector.connect(
    account=config['snowflake']['account'],
    user=config['snowflake']['user'],
    password=config['snowflake']['password'],
    warehouse=config['snowflake']['warehouse'],
    database=config['snowflake']['database'],
    schema=config['snowflake']['schema']
)

# Query loadtracker for packages
loadtracker = config['snowflake']['loadtracker_table']
query = f"""
SELECT 
    PACKAGENAME,
    COUNT(DISTINCT TABLENAME) as TABLE_COUNT,
    MAX(LASTTABLEUPDATEDATETIME) as LATEST_UPDATE
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
print(f"{'Package Name':<30} {'Tables':<10} {'Latest Update'}")
print("-" * 80)

packages = []
for row in cursor.fetchall():
    package_name, table_count, latest_update = row
    packages.append(package_name)
    print(f"{package_name:<30} {table_count:<10} {latest_update}")

print("-" * 80)
print(f"Total packages: {len(packages)}")
print()

if packages:
    print("To create base snapshot for a package, run:")
    print(f"  python create_base_snapshot.py --package {packages[0]} --config config.yaml")
else:
    print("WARNING: No packages found in loadtracker!")

conn.close()
