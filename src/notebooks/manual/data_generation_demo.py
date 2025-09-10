# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader Framework - Data Generation Demo
# MAGIC 
# MAGIC This notebook demonstrates the Auto Loader framework capabilities through four distinct scenarios:
# MAGIC 
# MAGIC ## Scenarios Overview
# MAGIC 
# MAGIC 1. **Customer Data (SCD Type 2)** - Incremental CSV files with historical tracking
# MAGIC 2. **Transaction Data (SCD Type 1)** - CSV files with soft delete flags
# MAGIC 3. **Inventory Data** - Daily CSV files with archive folder and corrupt data handling
# MAGIC 4. **Shipment Data** - File notification mode with evolving schema
# MAGIC 
# MAGIC ## How to Use This Notebook
# MAGIC 
# MAGIC 1. Run each cell sequentially to generate sample data
# MAGIC 2. Each scenario creates realistic data that demonstrates specific Auto Loader features
# MAGIC 3. Use the generated data to test your Auto Loader pipelines
# MAGIC 4. Modify the data generation parameters to create different test scenarios
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration
# MAGIC 
# MAGIC First, let's set up the environment and define our data generation parameters.
# MAGIC 
# MAGIC ### Volume Configuration
# MAGIC 
# MAGIC This notebook uses **Databricks Volumes** for storing generated data. Volumes provide:
# MAGIC - Better performance than DBFS
# MAGIC - Native integration with Unity Catalog
# MAGIC - Support for file notification mode
# MAGIC - Better security and governance
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Unity Catalog enabled workspace
# MAGIC - Permissions to create volumes in your catalog/schema
# MAGIC - Access to the specified catalog and schema
# MAGIC 
# MAGIC **Configuration:**
# MAGIC - Update `CATALOG` and `SCHEMA` variables below to match your environment
# MAGIC - The volume `autoloader_demo` will be created automatically

# COMMAND ----------

import pandas as pd
import numpy as np
import json
import random
from datetime import datetime, timedelta
from faker import Faker
import uuid
import os

# Initialize Faker for generating realistic data
fake = Faker()

print("All imports loaded successfully!")
print("Available functions:")
print("   • generate_customer_data() - Customer data with SCD Type 2")
print("   • generate_transaction_data() - Transaction data with SCD Type 1")
print("   • generate_inventory_data() - Inventory data with archive handling")
print("   • generate_shipment_data() - Shipment data with schema evolution")
print("   • cleanup_generated_data() - Reset all demo data")

# Configuration - Using Databricks Widgets for Environment-Specific Values
# These widgets can be set via bundle parameters or manually in the UI

# Create widgets for environment-specific configuration
dbutils.widgets.text("catalog_name", "main", "Catalog Name")
dbutils.widgets.text("schema_name", "autoloader_demo", "Schema Name") 
dbutils.widgets.text("volume_name", "raw_data", "Volume Name")

# Get values from widgets
CATALOG = dbutils.widgets.get("catalog_name")
SCHEMA = dbutils.widgets.get("schema_name")
VOLUME_NAME = dbutils.widgets.get("volume_name")

# Validate that all required parameters are provided
if not all([CATALOG, SCHEMA, VOLUME_NAME]):
    raise ValueError("Missing required parameters. Please set catalog_name, schema_name, and volume_name widgets.")

print(f"Using configuration:")
print(f"  Catalog: {CATALOG}")
print(f"  Schema: {SCHEMA}")
print(f"  Volume: {VOLUME_NAME}")

# Volume paths
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}"
CUSTOMER_PATH = f"{VOLUME_PATH}/customers"
TRANSACTION_PATH = f"{VOLUME_PATH}/transactions"
INVENTORY_PATH = f"{VOLUME_PATH}/inventory"
SHIPMENT_PATH = f"{VOLUME_PATH}/shipments"
PRODUCT_CATALOG_CDC_PATH = f"{VOLUME_PATH}/product_catalog_cdc"

# Create volume and directories
try:
    # Create the volume if it doesn't exist
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME}")
    print(f"Created/verified volume: {CATALOG}.{SCHEMA}.{VOLUME_NAME}")
    
    # Create data directories within the volume
    dbutils.fs.mkdirs(CUSTOMER_PATH)
    dbutils.fs.mkdirs(TRANSACTION_PATH)
    dbutils.fs.mkdirs(INVENTORY_PATH)
    dbutils.fs.mkdirs(SHIPMENT_PATH)
    dbutils.fs.mkdirs(PRODUCT_CATALOG_CDC_PATH)
    
    # Create checkpoint directories for Auto Loader state tracking
    # Note: Schema directories are not needed for fixed schema demos (customer, inventory)
    # as they use inline schemas defined in the TSV configuration
    
    # Checkpoint directories (for Auto Loader state tracking)
    dbutils.fs.mkdirs(f"{VOLUME_PATH}/checkpoint/customers")
    dbutils.fs.mkdirs(f"{VOLUME_PATH}/checkpoint/transactions")
    dbutils.fs.mkdirs(f"{VOLUME_PATH}/checkpoint/inventory")
    dbutils.fs.mkdirs(f"{VOLUME_PATH}/checkpoint/shipments")
    dbutils.fs.mkdirs(f"{VOLUME_PATH}/checkpoint/product_catalog_cdc")
    
    # Schema directories (only needed for schema inference demos)
    dbutils.fs.mkdirs(f"{VOLUME_PATH}/schema/transactions")
    dbutils.fs.mkdirs(f"{VOLUME_PATH}/schema/shipments")
    
    # Archive directory for inventory demo
    dbutils.fs.mkdirs(f"{VOLUME_PATH}/archive/inventory")
    
    print(f"✅ Created directories in volume: {VOLUME_PATH}")
    print(f"📁 Data directories:")
    print(f"   • Customer data: {CUSTOMER_PATH}")
    print(f"   • Transaction data: {TRANSACTION_PATH}")
    print(f"   • Inventory data: {INVENTORY_PATH}")
    print(f"   • Shipment data: {SHIPMENT_PATH}")
    print(f"   • Product Catalog CDC data: {PRODUCT_CATALOG_CDC_PATH}")
    print(f"🔄 Checkpoint directories (for Auto Loader state tracking):")
    print(f"   • {VOLUME_PATH}/checkpoint/customers")
    print(f"   • {VOLUME_PATH}/checkpoint/transactions")
    print(f"   • {VOLUME_PATH}/checkpoint/inventory")
    print(f"   • {VOLUME_PATH}/checkpoint/shipments")
    print(f"   • {VOLUME_PATH}/checkpoint/product_catalog_cdc")
    print(f"📋 Schema directories (for schema inference demos only):")
    print(f"   • {VOLUME_PATH}/schema/transactions")
    print(f"   • {VOLUME_PATH}/schema/shipments")
    print(f"📦 Archive directory:")
    print(f"   • {VOLUME_PATH}/archive/inventory")
    
    # Schema strategies for each demo:
    print(f"\nSchema Strategy:")
    print(f"   • Customer Demo: Fixed schema (inline in TSV config)")
    print(f"   • Inventory Demo: Fixed schema (inline in TSV config)")
    print(f"   • Transaction Demo: Schema inference (uses schema directory)")
    print(f"   • Shipment Demo: Schema evolution (uses schema directory)")
    
except Exception as e:
    print(f"Error creating volume or directories: {e}")
    print("Please ensure you have the necessary permissions to create volumes and directories.")
    print("You may need to create the volume manually or adjust the catalog/schema names.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Management
# MAGIC 
# MAGIC **Note**: This demo now uses inline schemas defined in the TSV configuration file.
# MAGIC The schemas are automatically applied by the Auto Loader pipelines, so no manual
# MAGIC schema file creation is needed.
# MAGIC 
# MAGIC - **Customer Demo**: Uses fixed schema with corrupt data handling
# MAGIC - **Inventory Demo**: Uses fixed schema with corrupt data handling  
# MAGIC - **Transaction Demo**: Uses schema inference
# MAGIC - **Shipment Demo**: Uses schema evolution mode

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Scenario 1: Customer Data (SCD Type 2)
# MAGIC 
# MAGIC **Features Demonstrated:**
# MAGIC - Incremental CSV file ingestion
# MAGIC - SCD Type 2 implementation in Silver layer
# MAGIC - Historical data tracking with effective dates
# MAGIC 
# MAGIC **Data Characteristics:**
# MAGIC - CSV format with headers
# MAGIC - Incremental updates (only changed records)
# MAGIC - Customer information changes over time
# MAGIC - Includes effective date and end date for SCD Type 2

# COMMAND ----------

def generate_customer_data(batch_number=1, num_customers=50, include_updates=True):
    """
    Generate customer data for SCD Type 2 scenario
    
    Args:
        batch_number: Batch number for file naming
        num_customers: Number of customers to generate
        include_updates: Whether to include updates to existing customers
    """
    
    # Ensure imports are available (fallback if imports cell wasn't run)
    try:
        from datetime import datetime, timedelta
        from faker import Faker
        fake = Faker()
    except ImportError:
        print("Error: Required imports not available. Please run the imports cell first.")
        return None
    
    customers = []
    base_date = datetime.now() - timedelta(days=30)
    
    for i in range(num_customers):
        customer_id = f"CUST_{1000 + i:04d}"
        
        # Generate base customer data
        customer = {
            'customer_id': customer_id,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'phone_number': fake.phone_number()[:15],  # Limit phone length
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode(),
            'country': 'USA',
            'customer_tier': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum']),
            'registration_date': (base_date + timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
            'update_ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # For batch 2+, include some updates to existing customers
        if batch_number > 1 and include_updates and random.random() < 0.3:
            # This represents an update to an existing customer
            customer['customer_id'] = f"CUST_{1000 + random.randint(0, 49):04d}"
            customer['update_ts'] = (datetime.now() + timedelta(hours=random.randint(1, 24))).strftime('%Y-%m-%d %H:%M:%S')
        
        customers.append(customer)
    
    return pd.DataFrame(customers)

# Generate initial customer data (Batch 1)
print("Generating Customer Data - Batch 1 (Initial Load)")
customer_batch1 = generate_customer_data(batch_number=1, num_customers=50, include_updates=False)

# Save to CSV in volume
customer_file1 = f"{CUSTOMER_PATH}/customers_batch_001.csv"
customer_batch1.to_csv(f"{customer_file1}", index=False)

print(f"Generated {len(customer_batch1)} customer records")
print(f"Saved to: {customer_file1}")
print(f"Sample data:")
display(customer_batch1.head())

# COMMAND ----------

# Generate customer data updates (Batch 2)
print("Generating Customer Data - Batch 2 (Updates)")
customer_batch2 = generate_customer_data(batch_number=2, num_customers=20, include_updates=True)

# Save to CSV
customer_file2 = f"{CUSTOMER_PATH}/customers_batch_002.csv"
customer_batch2.to_csv(f"{customer_file2}", index=False)

print(f"Generated {len(customer_batch2)} customer update records")
print(f"Saved to: {customer_file2}")
print(f"Sample updates:")
display(customer_batch2.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Scenario 2: Transaction Data (SCD Type 1)
# MAGIC 
# MAGIC **Features Demonstrated:**
# MAGIC - CSV file ingestion with soft delete flags
# MAGIC - SCD Type 1 implementation (overwrite changes)
# MAGIC - Soft delete handling
# MAGIC 
# MAGIC **Data Characteristics:**
# MAGIC - CSV format with headers
# MAGIC - Transaction records with soft delete flags
# MAGIC - Updates overwrite previous values (SCD Type 1)
# MAGIC - Includes is_deleted flag for soft deletes

# COMMAND ----------

def generate_transaction_data(batch_number=1, num_transactions=100, include_deletes=True):
    """
    Generate transaction data for SCD Type 1 scenario with soft deletes
    
    Args:
        batch_number: Batch number for file naming
        num_transactions: Number of transactions to generate
        include_deletes: Whether to include soft deleted records
    """
    
    # Ensure imports are available (fallback if imports cell wasn't run)
    try:
        from datetime import datetime, timedelta
        from faker import Faker
        fake = Faker()
    except ImportError:
        print("Error: Required imports not available. Please run the imports cell first.")
        return None
    
    transactions = []
    base_date = datetime.now() - timedelta(days=60)
    
    # Transaction types and statuses
    transaction_types = ['Purchase', 'Refund', 'Exchange', 'Return', 'Adjustment']
    statuses = ['Completed', 'Pending', 'Failed', 'Cancelled']
    
    for i in range(num_transactions):
        transaction_id = f"TXN_{2000 + i:06d}"
        customer_id = f"CUST_{1000 + random.randint(0, 49):04d}"
        
        # Generate base transaction data
        transaction = {
            'transaction_id': transaction_id,
            'customer_id': customer_id,
            'transaction_type': random.choice(transaction_types),
            'amount': round(random.uniform(10.0, 1000.0), 2),
            'currency': 'USD',
            'status': random.choice(statuses),
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer']),
            'transaction_date': (base_date + timedelta(days=random.randint(0, 60))).strftime('%Y-%m-%d'),
            'processed_date': (base_date + timedelta(days=random.randint(0, 60), hours=random.randint(0, 23))).strftime('%Y-%m-%d %H:%M:%S'),
            'is_deleted': False,
            'created_ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'updated_ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # For batch 2+, include some updates and soft deletes
        if batch_number > 1:
            if include_deletes and random.random() < 0.1:  # 10% soft deletes
                transaction['transaction_id'] = f"TXN_{2000 + random.randint(0, 99):06d}"
                transaction['is_deleted'] = True
                transaction['updated_ts'] = (datetime.now() + timedelta(hours=random.randint(1, 24))).strftime('%Y-%m-%d %H:%M:%S')
            elif random.random() < 0.2:  # 20% updates
                transaction['transaction_id'] = f"TXN_{2000 + random.randint(0, 99):06d}"
                transaction['status'] = random.choice(['Completed', 'Failed', 'Cancelled'])
                transaction['updated_ts'] = (datetime.now() + timedelta(hours=random.randint(1, 24))).strftime('%Y-%m-%d %H:%M:%S')
        
        transactions.append(transaction)
    
    return pd.DataFrame(transactions)

# Generate initial transaction data (Batch 1)
print("Generating Transaction Data - Batch 1 (Initial Load)")
transaction_batch1 = generate_transaction_data(batch_number=1, num_transactions=100, include_deletes=False)

# Save to CSV
transaction_file1 = f"{TRANSACTION_PATH}/transactions_batch_001.csv"
transaction_batch1.to_csv(f"{transaction_file1}", index=False)

print(f"Generated {len(transaction_batch1)} transaction records")
print(f"Saved to: {transaction_file1}")
print(f"Sample data:")
display(transaction_batch1.head())

# COMMAND ----------

# Generate transaction data updates and soft deletes (Batch 2)
print("Generating Transaction Data - Batch 2 (Updates & Soft Deletes)")
transaction_batch2 = generate_transaction_data(batch_number=2, num_transactions=30, include_deletes=True)

# Save to CSV
transaction_file2 = f"{TRANSACTION_PATH}/transactions_batch_002.csv"
transaction_batch2.to_csv(f"{transaction_file2}", index=False)

print(f"Generated {len(transaction_batch2)} transaction update/delete records")
print(f"Saved to: {transaction_file2}")
print(f"Sample updates and deletes:")
display(transaction_batch2.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Scenario 3: Inventory Data (Archive & Corrupt Data Handling)
# MAGIC 
# MAGIC **Features Demonstrated:**
# MAGIC - Daily CSV file ingestion
# MAGIC - Archive folder functionality
# MAGIC - Corrupt data handling with corrupt data column
# MAGIC - Fixed schema validation
# MAGIC 
# MAGIC **Data Characteristics:**
# MAGIC - CSV format with fixed schema
# MAGIC - Daily files with date-based naming
# MAGIC - Some files contain corrupt data for testing
# MAGIC - Files are moved to archive folder after processing

# COMMAND ----------

def generate_inventory_data(date_str, num_products=200, include_corrupt=False):
    """
    Generate inventory data for archive and corrupt data handling scenario
    
    Args:
        date_str: Date string for file naming (YYYY-MM-DD)
        num_products: Number of products to generate
        include_corrupt: Whether to include corrupt data
    """
    
    # Ensure imports are available (fallback if imports cell wasn't run)
    try:
        from datetime import datetime, timedelta
        from faker import Faker
        import pandas as pd
        fake = Faker()
    except ImportError:
        print("Error: Required imports not available. Please run the imports cell first.")
        return None
    
    inventory = []
    categories = ['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports', 'Toys']
    brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE']
    
    for i in range(num_products):
        product_id = f"PROD_{3000 + i:06d}"
        
        # Generate base inventory data
        inventory_record = {
            'product_id': product_id,
            'product_name': fake.catch_phrase(),
            'category': random.choice(categories),
            'brand': random.choice(brands),
            'sku': f"SKU-{random.randint(100000, 999999)}",
            'quantity_on_hand': random.randint(0, 1000),
            'quantity_reserved': random.randint(0, 100),
            'unit_cost': round(random.uniform(5.0, 500.0), 2),
            'unit_price': round(random.uniform(10.0, 1000.0), 2),
            'warehouse_location': random.choice(['Warehouse-A', 'Warehouse-B', 'Warehouse-C']),
            'last_restocked': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
            'reorder_level': random.randint(10, 100),
            'supplier_id': f"SUPP_{random.randint(1, 20):03d}",
            'batch_date': date_str,
            'created_ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Introduce corrupt data for testing
        if include_corrupt and random.random() < 0.05:  # 5% corrupt records
            # Corrupt the quantity field (non-numeric)
            inventory_record['quantity_on_hand'] = 'INVALID_QUANTITY'
            # Corrupt the price field (negative value)
            inventory_record['unit_price'] = -999.99
            # Add invalid date
            inventory_record['last_restocked'] = 'INVALID_DATE'
        
        inventory.append(inventory_record)
    
    df = pd.DataFrame(inventory)
    
    # Ensure proper data types for PySpark compatibility
    df['quantity_on_hand'] = pd.to_numeric(df['quantity_on_hand'], errors='coerce')
    df['quantity_reserved'] = pd.to_numeric(df['quantity_reserved'], errors='coerce')
    df['unit_cost'] = pd.to_numeric(df['unit_cost'], errors='coerce')
    df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    df['reorder_level'] = pd.to_numeric(df['reorder_level'], errors='coerce')
    
    # Convert date columns to proper datetime
    df['last_restocked'] = pd.to_datetime(df['last_restocked'], errors='coerce')
    df['created_ts'] = pd.to_datetime(df['created_ts'], errors='coerce')
    
    return df

# Generate inventory data for multiple days
print("Generating Inventory Data - Multiple Days")

# Generate data for the last 5 days
for days_ago in range(5, 0, -1):
    date = datetime.now() - timedelta(days=days_ago)
    date_str = date.strftime('%Y-%m-%d')
    
    # Include corrupt data in some files
    include_corrupt = days_ago in [3, 1]  # Corrupt data in day 3 and day 1
    
    inventory_data = generate_inventory_data(
        date_str=date_str, 
        num_products=200, 
        include_corrupt=include_corrupt
    )
    
    # Save to CSV with date-based naming
    inventory_file = f"{INVENTORY_PATH}/inventory_{date_str}.csv"
    inventory_data.to_csv(f"{inventory_file}", index=False)
    
    corrupt_count = len(inventory_data[inventory_data['quantity_on_hand'].isna()])
    print(f"Generated inventory data for {date_str}: {len(inventory_data)} records ({corrupt_count} corrupt)")

print(f"All inventory files saved to: {INVENTORY_PATH}")
print(f"Sample inventory data:")
display(inventory_data.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Scenario 4: Shipment Data (File Notification Mode & Evolving Schema)
# MAGIC 
# MAGIC **Features Demonstrated:**
# MAGIC - File notification mode for real-time processing
# MAGIC - Evolving schema handling
# MAGIC - JSON format with schema evolution
# MAGIC - Auto Loader with managed file events
# MAGIC 
# MAGIC **Data Characteristics:**
# MAGIC - JSON format for schema flexibility
# MAGIC - Schema evolves over time (new fields added)
# MAGIC - Real-time file drops for notification testing
# MAGIC - Includes tracking information and delivery updates

# COMMAND ----------

def generate_shipment_data(batch_number=1, num_shipments=75, schema_version=1):
    """
    Generate shipment data with evolving schema for file notification mode
    
    Args:
        batch_number: Batch number for file naming
        num_shipments: Number of shipments to generate
        schema_version: Schema version (1, 2, or 3) for evolution testing
    """
    
    # Ensure imports are available (fallback if imports cell wasn't run)
    try:
        from datetime import datetime, timedelta
        from faker import Faker
        fake = Faker()
    except ImportError:
        print("Error: Required imports not available. Please run the imports cell first.")
        return None
    
    shipments = []
    carriers = ['UPS', 'FedEx', 'DHL', 'USPS', 'Amazon Logistics']
    statuses = ['Pending', 'In Transit', 'Out for Delivery', 'Delivered', 'Exception']
    
    for i in range(num_shipments):
        shipment_id = f"SHIP_{4000 + i:06d}"
        order_id = f"ORD_{5000 + random.randint(0, 999):06d}"
        
        # Base shipment data (Schema Version 1)
        shipment = {
            'shipment_id': shipment_id,
            'order_id': order_id,
            'carrier': random.choice(carriers),
            'tracking_number': f"TRK{random.randint(100000000, 999999999)}",
            'status': random.choice(statuses),
            'origin_address': fake.address(),
            'destination_address': fake.address(),
            'ship_date': (datetime.now() - timedelta(days=random.randint(0, 10))).strftime('%Y-%m-%d'),
            'estimated_delivery': (datetime.now() + timedelta(days=random.randint(1, 7))).strftime('%Y-%m-%d'),
            'weight_lbs': round(random.uniform(0.5, 50.0), 2),
            'created_ts': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Schema Version 2: Add new fields
        if schema_version >= 2:
            shipment.update({
                'package_dimensions': {
                    'length': round(random.uniform(5.0, 48.0), 1),
                    'width': round(random.uniform(5.0, 48.0), 1),
                    'height': round(random.uniform(2.0, 24.0), 1)
                },
                'insurance_value': round(random.uniform(0.0, 1000.0), 2),
                'signature_required': random.choice([True, False]),
                'delivery_instructions': fake.text(max_nb_chars=100) if random.random() < 0.3 else None
            })
        
        # Schema Version 3: Add more fields
        if schema_version >= 3:
            shipment.update({
                'temperature_controlled': random.choice([True, False]),
                'hazardous_materials': random.choice([True, False]),
                'customs_declaration': {
                    'declared_value': round(random.uniform(10.0, 500.0), 2),
                    'country_of_origin': random.choice(['USA', 'China', 'Mexico', 'Canada', 'Germany']),
                    'hs_code': f"{random.randint(10, 99)}{random.randint(10, 99)}{random.randint(10, 99)}{random.randint(10, 99)}"
                },
                'real_time_tracking': {
                    'last_scan_location': fake.city(),
                    'last_scan_time': (datetime.now() - timedelta(hours=random.randint(1, 48))).strftime('%Y-%m-%d %H:%M:%S'),
                    'scan_type': random.choice(['Pickup', 'In Transit', 'Out for Delivery', 'Delivered', 'Exception'])
                }
            })
        
        shipments.append(shipment)
    
    return shipments

# Generate shipment data with different schema versions
print("Generating Shipment Data - Schema Evolution")

# Batch 1: Schema Version 1 (Basic fields)
shipment_batch1 = generate_shipment_data(batch_number=1, num_shipments=75, schema_version=1)
shipment_file1 = f"{SHIPMENT_PATH}/shipments_batch_001.json"
with open(f"{shipment_file1}", 'w') as f:
    json.dump(shipment_batch1, f, indent=2)

print(f"Generated {len(shipment_batch1)} shipment records (Schema v1)")
print(f"Saved to: {shipment_file1}")

# Batch 2: Schema Version 2 (Added dimensions, insurance, etc.)
shipment_batch2 = generate_shipment_data(batch_number=2, num_shipments=50, schema_version=2)
shipment_file2 = f"{SHIPMENT_PATH}/shipments_batch_002.json"
with open(f"{shipment_file2}", 'w') as f:
    json.dump(shipment_batch2, f, indent=2)

print(f"Generated {len(shipment_batch2)} shipment records (Schema v2)")
print(f"Saved to: {shipment_file2}")

# Batch 3: Schema Version 3 (Added customs, real-time tracking)
shipment_batch3 = generate_shipment_data(batch_number=3, num_shipments=40, schema_version=3)
shipment_file3 = f"{SHIPMENT_PATH}/shipments_batch_003.json"
with open(f"{shipment_file3}", 'w') as f:
    json.dump(shipment_batch3, f, indent=2)

print(f"Generated {len(shipment_batch3)} shipment records (Schema v3)")
print(f"Saved to: {shipment_file3}")

print(f"All shipment files saved to: {SHIPMENT_PATH}")
print(f"Sample shipment data (Schema v3):")
display(pd.DataFrame([shipment_batch3[0]]))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Scenario 5: Product Catalog CDC (Change Data Capture with Timestamps)
# MAGIC 
# MAGIC **Features Demonstrated:**
# MAGIC - CDC data with create, updated, deleted timestamps
# MAGIC - Custom expressions for CDC operation detection
# MAGIC - Business column mapping in silver layer
# MAGIC - SCD Type 2 with CDC-aware processing
# MAGIC 
# MAGIC **Data Characteristics:**
# MAGIC - CSV format with CDC timestamp columns
# MAGIC - Realistic product catalog data
# MAGIC - Multiple CDC operations (INSERT, UPDATE, DELETE)
# MAGIC - Custom expressions for preprocessing and mapping

# COMMAND ----------

def generate_product_catalog_cdc_data(batch_number=1, num_products=100, include_deletes=True):
    """
    Generate product catalog CDC data with create, updated, deleted timestamps
    
    Args:
        batch_number: Batch number for file naming
        num_products: Number of products to generate
        include_deletes: Whether to include soft deleted records
    """
    
    # Ensure imports are available (fallback if imports cell wasn't run)
    try:
        from datetime import datetime, timedelta
        from faker import Faker
        fake = Faker()
    except ImportError:
        print("Error: Required imports not available. Please run the imports cell first.")
        return None
    
    products = []
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Automotive', 'Health & Beauty']
    statuses = ['active', 'inactive', 'discontinued']
    
    base_date = datetime.now() - timedelta(days=90)
    
    for i in range(num_products):
        product_id = f"PROD_{5000 + i:06d}"
        
        # Generate base product data
        created_at = base_date + timedelta(days=random.randint(0, 90))
        updated_at = created_at + timedelta(hours=random.randint(0, 24))
        
        product = {
            'product_id': product_id,
            'product_name': fake.catch_phrase(),
            'category': random.choice(categories),
            'price': round(random.uniform(10.0, 2000.0), 2),
            'description': fake.text(max_nb_chars=200),
            'status': random.choice(statuses),
            'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'updated_at': updated_at.strftime('%Y-%m-%d %H:%M:%S'),
            'deleted_at': None
        }
        
        # For batch 2+, include updates and soft deletes
        if batch_number > 1:
            if include_deletes and random.random() < 0.15:  # 15% soft deletes
                # Soft delete an existing product
                product['product_id'] = f"PROD_{5000 + random.randint(0, 99):06d}"
                product['deleted_at'] = (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d %H:%M:%S')
                product['status'] = 'inactive'
                product['updated_at'] = product['deleted_at']
            elif random.random() < 0.4:  # 40% updates
                # Update an existing product
                product['product_id'] = f"PROD_{5000 + random.randint(0, 99):06d}"
                product['price'] = round(random.uniform(10.0, 2000.0), 2)
                product['status'] = random.choice(['active', 'inactive'])
                product['updated_at'] = (datetime.now() - timedelta(days=random.randint(1, 30))).strftime('%Y-%m-%d %H:%M:%S')
                product['created_at'] = (datetime.now() - timedelta(days=random.randint(30, 90))).strftime('%Y-%m-%d %H:%M:%S')
        
        products.append(product)
    
    return pd.DataFrame(products)

# Generate initial product catalog CDC data (Batch 1)
print("Generating Product Catalog CDC Data - Batch 1 (Initial Load)")
product_cdc_batch1 = generate_product_catalog_cdc_data(batch_number=1, num_products=100, include_deletes=False)

# Save to CSV
product_cdc_file1 = f"{PRODUCT_CATALOG_CDC_PATH}/product_catalog_cdc_batch_001.csv"
product_cdc_batch1.to_csv(f"{product_cdc_file1}", index=False)

print(f"Generated {len(product_cdc_batch1)} product records")
print(f"Saved to: {product_cdc_file1}")
print(f"Sample data:")
display(product_cdc_batch1.head())

# COMMAND ----------

# Generate product catalog CDC data updates and soft deletes (Batch 2)
print("Generating Product Catalog CDC Data - Batch 2 (Updates & Soft Deletes)")
product_cdc_batch2 = generate_product_catalog_cdc_data(batch_number=2, num_products=50, include_deletes=True)

# Save to CSV
product_cdc_file2 = f"{PRODUCT_CATALOG_CDC_PATH}/product_catalog_cdc_batch_002.csv"
product_cdc_batch2.to_csv(f"{product_cdc_file2}", index=False)

print(f"Generated {len(product_cdc_batch2)} product update/delete records")
print(f"Saved to: {product_cdc_file2}")
print(f"Sample updates and deletes:")
display(product_cdc_batch2.head())

# COMMAND ----------

# Generate additional product catalog CDC data (Batch 3)
print("Generating Product Catalog CDC Data - Batch 3 (More Updates)")
product_cdc_batch3 = generate_product_catalog_cdc_data(batch_number=3, num_products=30, include_deletes=True)

# Save to CSV
product_cdc_file3 = f"{PRODUCT_CATALOG_CDC_PATH}/product_catalog_cdc_batch_003.csv"
product_cdc_batch3.to_csv(f"{product_cdc_file3}", index=False)

print(f"Generated {len(product_cdc_batch3)} product update/delete records")
print(f"Saved to: {product_cdc_file3}")
print(f"Sample updates and deletes:")
display(product_cdc_batch3.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary and Next Steps
# MAGIC 
# MAGIC ### Generated Data Summary
# MAGIC 
# MAGIC | Scenario | Data Type | Files Generated | Key Features |
# MAGIC |----------|-----------|-----------------|--------------|
# MAGIC | **Customer Data** | CSV | 2 files | SCD Type 2, Incremental updates |
# MAGIC | **Transaction Data** | CSV | 2 files | SCD Type 1, Soft deletes |
# MAGIC | **Inventory Data** | CSV | 5 files | Daily files, Corrupt data handling |
# MAGIC | **Shipment Data** | JSON | 3 files | Schema evolution, File notifications |
# MAGIC | **Product Catalog CDC** | CSV | 3 files | CDC timestamps, Custom expressions, Business mapping |
# MAGIC 
# MAGIC ### File Locations (Databricks Volumes)
# MAGIC - **Customer Data**: `/Volumes/vbdemos/dbdemos_autoloader/raw_data/customers/`
# MAGIC - **Transaction Data**: `/Volumes/vbdemos/dbdemos_autoloader/raw_data/transactions/`
# MAGIC - **Inventory Data**: `/Volumes/vbdemos/dbdemos_autoloader/raw_data/inventory/`
# MAGIC - **Shipment Data**: `/Volumes/vbdemos/dbdemos_autoloader/raw_data/shipments/`
# MAGIC - **Product Catalog CDC Data**: `/Volumes/vbdemos/dbdemos_autoloader/raw_data/product_catalog_cdc/`
# MAGIC 
# MAGIC **Note**: These paths match the TSV configuration. Update the CATALOG, SCHEMA, and VOLUME_NAME variables above if you need different paths.
# MAGIC 
# MAGIC ### Next Steps for Testing
# MAGIC 
# MAGIC 1. **Configure Auto Loader Pipelines**: Use the generated data paths in your Auto Loader configurations
# MAGIC 2. **Test SCD Implementations**: 
# MAGIC    - Customer data for SCD Type 2 (historical tracking)
# MAGIC    - Transaction data for SCD Type 1 (overwrite changes)
# MAGIC 3. **Test Error Handling**: 
# MAGIC    - Inventory data with corrupt records
# MAGIC    - Archive folder functionality
# MAGIC 4. **Test Schema Evolution**: 
# MAGIC    - Shipment data with evolving JSON schema
# MAGIC    - File notification mode setup
# MAGIC 
# MAGIC ### Auto Loader Configuration Examples
# MAGIC 
# MAGIC For each scenario, you can now configure Auto Loader with:
# MAGIC - **Source paths**: Use the generated volume paths above
# MAGIC - **File formats**: CSV for scenarios 1-3, JSON for scenario 4
# MAGIC - **Schema handling**: 
# MAGIC   - Customer: Fixed schema (inline in TSV config with corrupt data handling)
# MAGIC   - Inventory: Fixed schema (inline in TSV config with corrupt data handling)
# MAGIC   - Transaction: `inferSchema=true` (Auto Loader infers schema)
# MAGIC   - Shipment: `schemaEvolutionMode=rescue` (Schema evolves dynamically)
# MAGIC - **Error handling**: Corrupt data column for inventory data
# MAGIC - **Archive options**: Move processed files to archive folders
# MAGIC - **File notifications**: Enable for shipment data processing
# MAGIC 
# MAGIC **Volume Path Examples for Auto Loader:**
# MAGIC ```python
# MAGIC # Customer data (Fixed schema with corrupt data handling)
# MAGIC source_path = "/Volumes/vbdemos/dbdemos_autoloader/raw_data/customers"
# MAGIC 
# MAGIC # Transaction data (Schema inference)
# MAGIC source_path = "/Volumes/vbdemos/dbdemos_autoloader/raw_data/transactions"
# MAGIC 
# MAGIC # Inventory data (Fixed schema with corrupt data handling)
# MAGIC source_path = "/Volumes/vbdemos/dbdemos_autoloader/raw_data/inventory"
# MAGIC 
# MAGIC # Shipment data (Schema evolution & File notifications)
# MAGIC source_path = "/Volumes/vbdemos/dbdemos_autoloader/raw_data/shipments"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Generation Complete! 🎉
# MAGIC 
# MAGIC All sample data has been generated and is ready for Auto Loader testing. You can now:
# MAGIC 
# MAGIC 1. **Run this notebook** to generate fresh data whenever needed
# MAGIC 2. **Modify the parameters** in each cell to create different test scenarios
# MAGIC 3. **Use the generated data paths** in your Auto Loader pipeline configurations
# MAGIC 4. **Test the four scenarios** step by step to understand Auto Loader capabilities
# MAGIC 
# MAGIC Happy testing! 🚀

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Additional Data Generation Utilities
# MAGIC 
# MAGIC The following cells provide additional utilities for generating more test data or modifying existing data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Additional Customer Updates
# MAGIC 
# MAGIC Use this cell to generate more customer update batches for testing SCD Type 2 scenarios.

# COMMAND ----------

def generate_additional_customer_batch(batch_number, num_customers=25):
    """Generate additional customer update batch"""
    print(f"Generating Customer Data - Batch {batch_number:03d}")
    
    customers = generate_customer_data(batch_number=batch_number, num_customers=num_customers, include_updates=True)
    
    # Save to CSV
    customer_file = f"{CUSTOMER_PATH}/customers_batch_{batch_number:03d}.csv"
    customers.to_csv(f"{customer_file}", index=False)
    
    print(f"Generated {len(customers)} customer records")
    print(f"Saved to: {customer_file}")
    
    return customers

# Example: Generate batch 3
# additional_customers = generate_additional_customer_batch(3, 25)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Additional Transaction Updates
# MAGIC 
# MAGIC Use this cell to generate more transaction update batches for testing SCD Type 1 scenarios.

# COMMAND ----------

def generate_additional_transaction_batch(batch_number, num_transactions=20):
    """Generate additional transaction update batch"""
    print(f"Generating Transaction Data - Batch {batch_number:03d}")
    
    transactions = generate_transaction_data(batch_number=batch_number, num_transactions=num_transactions, include_deletes=True)
    
    # Save to CSV
    transaction_file = f"{TRANSACTION_PATH}/transactions_batch_{batch_number:03d}.csv"
    transactions.to_csv(f"{transaction_file}", index=False)
    
    print(f"Generated {len(transactions)} transaction records")
    print(f"Saved to: {transaction_file}")
    
    return transactions

# Example: Generate batch 3
# additional_transactions = generate_additional_transaction_batch(3, 20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Additional Inventory Data
# MAGIC 
# MAGIC Use this cell to generate inventory data for additional dates.

# COMMAND ----------

def generate_additional_inventory_data(date_str, num_products=150, include_corrupt=False):
    """Generate inventory data for a specific date"""
    print(f"Generating Inventory Data for {date_str}")
    
    inventory = generate_inventory_data(date_str=date_str, num_products=num_products, include_corrupt=include_corrupt)
    
    # Save to CSV
    inventory_file = f"{INVENTORY_PATH}/inventory_{date_str}.csv"
    inventory.to_csv(f"{inventory_file}", index=False)
    
    corrupt_count = len(inventory[inventory['quantity_on_hand'].isna()])
    print(f"Generated {len(inventory)} inventory records ({corrupt_count} corrupt)")
    print(f"Saved to: {inventory_file}")
    
    return inventory

# Example: Generate inventory for a specific date
# specific_date = "2024-01-15"
# additional_inventory = generate_additional_inventory_data(specific_date, 150, include_corrupt=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Additional Shipment Data
# MAGIC 
# MAGIC Use this cell to generate shipment data with specific schema versions.

# COMMAND ----------

def generate_additional_shipment_batch(batch_number, num_shipments=30, schema_version=2):
    """Generate additional shipment batch with specific schema version"""
    print(f"Generating Shipment Data - Batch {batch_number:03d} (Schema v{schema_version})")
    
    shipments = generate_shipment_data(batch_number=batch_number, num_shipments=num_shipments, schema_version=schema_version)
    
    # Save to JSON
    shipment_file = f"{SHIPMENT_PATH}/shipments_batch_{batch_number:03d}.json"
    with open(f"{shipment_file}", 'w') as f:
        json.dump(shipments, f, indent=2)
    
    print(f"Generated {len(shipments)} shipment records (Schema v{schema_version})")
    print(f"Saved to: {shipment_file}")
    
    return shipments

# Example: Generate batch 4 with schema version 2
# additional_shipments = generate_additional_shipment_batch(4, 30, schema_version=2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Up Generated Data
# MAGIC 
# MAGIC Use this cell to clean up all generated test data.

# COMMAND ----------

def cleanup_generated_data():
    """Clean up all generated test data from the volume and reset directories"""
    print("🧹 Cleaning up generated test data from volume...")
    
    try:
        # Remove all generated files from the main volume
        dbutils.fs.rm(VOLUME_PATH, True)
        print(f"Removed all data from volume: {VOLUME_PATH}")
        
        # Remove customer demo data from the separate volume structure
        customer_volume_path = "/Volumes/vbdemos/dbdemos_autoloader/raw_data"
        if dbutils.fs.ls("/Volumes/vbdemos/dbdemos_autoloader/").count > 0:
            dbutils.fs.rm(customer_volume_path, True)
            print(f"Removed all data from customer volume: {customer_volume_path}")
        
        # Recreate all necessary directories
        print("Recreating directory structure...")
        
        # Recreate main volume directories
        dbutils.fs.mkdirs(CUSTOMER_PATH)
        dbutils.fs.mkdirs(TRANSACTION_PATH)
        dbutils.fs.mkdirs(INVENTORY_PATH)
        dbutils.fs.mkdirs(SHIPMENT_PATH)
        
        # Recreate checkpoint directories (for Auto Loader state tracking)
        dbutils.fs.mkdirs(f"{VOLUME_PATH}/checkpoint/customers")
        dbutils.fs.mkdirs(f"{VOLUME_PATH}/checkpoint/transactions")
        dbutils.fs.mkdirs(f"{VOLUME_PATH}/checkpoint/inventory")
        dbutils.fs.mkdirs(f"{VOLUME_PATH}/checkpoint/shipments")
        
        # Recreate schema directories (only needed for schema inference demos)
        dbutils.fs.mkdirs(f"{VOLUME_PATH}/schema/transactions")
        dbutils.fs.mkdirs(f"{VOLUME_PATH}/schema/shipments")
        
        # Recreate archive directory
        dbutils.fs.mkdirs(f"{VOLUME_PATH}/archive/inventory")
        
        print("Recreated all directories")
        print("🎉 Cleanup complete! Ready for fresh data generation.")
        
    except Exception as e:
        print(f"Error during cleanup: {e}")
        print("Note: The volumes themselves will remain, only the data files are removed.")

# Uncomment the line below to run cleanup
# cleanup_generated_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧹 Reset Demo Data
# MAGIC 
# MAGIC **To reset all demo data and start fresh:**
# MAGIC 
# MAGIC 1. Uncomment the line below
# MAGIC 2. Run this cell
# MAGIC 3. This will remove all generated data and recreate the directory structure
# MAGIC 
# MAGIC **⚠️ Warning:** This will delete all generated test data!

# COMMAND ----------

# Uncomment the line below to reset all demo data
# cleanup_generated_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## End of Data Generation Demo
# MAGIC 
# MAGIC This notebook provides a comprehensive data generation framework for testing Auto Loader scenarios. 
# MAGIC 
# MAGIC **Remember to:**
# MAGIC 1. Run cells sequentially for best results
# MAGIC 2. Modify parameters as needed for your specific test cases
# MAGIC 3. Use the generated data paths in your Auto Loader configurations
# MAGIC 4. Test each scenario step by step to understand Auto Loader capabilities
# MAGIC 
# MAGIC **For more information about Auto Loader:**
# MAGIC - [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)
# MAGIC - [File Notification Mode](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/file-notification-mode#file-events)
# MAGIC - [Schema Evolution](https://docs.databricks.com/ingestion/auto-loader/schema-evolution.html)
# MAGIC 
# MAGIC Happy testing! 🚀
