#!/usr/bin/env python3
"""
Script to generate Parquet files from sample data for testing the autoloader framework.
"""

import pandas as pd
import json
from pathlib import Path

def create_customers_parquet():
    """Create customers.parquet from CSV data."""
    df = pd.read_csv('customers.csv')
    df['update_ts'] = pd.to_datetime(df['update_ts'])
    df.to_parquet('customers.parquet', index=False)
    print("✅ Created customers.parquet")

def create_orders_parquet():
    """Create orders.parquet from JSON data."""
    orders = []
    with open('orders.json', 'r') as f:
        for line in f:
            if line.strip():
                orders.append(json.loads(line))
    
    df = pd.DataFrame(orders)
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['created_ts'] = pd.to_datetime(df['created_ts'])
    df.to_parquet('orders.parquet', index=False)
    print("✅ Created orders.parquet")

def create_order_items_parquet():
    """Create order_items.parquet from JSON data."""
    items = []
    with open('order_items.json', 'r') as f:
        for line in f:
            if line.strip():
                items.append(json.loads(line))
    
    df = pd.DataFrame(items)
    df['created_ts'] = pd.to_datetime(df['created_ts'])
    df.to_parquet('order_items.parquet', index=False)
    print("✅ Created order_items.parquet")

def create_products_parquet():
    """Create products.parquet from CSV data."""
    df = pd.read_csv('products.csv')
    df['created_date'] = pd.to_datetime(df['created_date'])
    df['last_updated'] = pd.to_datetime(df['last_updated'])
    df.to_parquet('products.parquet', index=False)
    print("✅ Created products.parquet")

def main():
    """Generate all Parquet files."""
    print("🔄 Generating Parquet files from sample data...")
    
    # Change to sample_data directory
    sample_dir = Path(__file__).parent
    os.chdir(sample_dir)
    
    try:
        create_customers_parquet()
        create_orders_parquet()
        create_order_items_parquet()
        create_products_parquet()
        
        print("\n🎉 All Parquet files generated successfully!")
        print("\n📁 Generated files:")
        print("  - customers.parquet")
        print("  - orders.parquet") 
        print("  - order_items.parquet")
        print("  - products.parquet")
        
    except Exception as e:
        print(f"❌ Error generating Parquet files: {e}")

if __name__ == "__main__":
    import os
    main()
