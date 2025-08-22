# Sample Data for Testing

This directory contains sample data files for testing the autoloader framework with Azure Data Lake Storage.

## 📁 Files Included

### CSV Files
- `customers.csv` - Customer demographics (10 records)
- `products.csv` - Product catalog (10 records)

### JSON Files  
- `orders.json` - Order records with nested structure (10 records)
- `order_items.json` - Order line items (10 records)

### Parquet Files
- `*.parquet` - Same data in Parquet format for performance testing

## ⚙️ Configuration Files

### `test_config_adls.tsv`
Ready-to-use configuration for ADLS testing:
- Pre-configured ADLS Gen2 paths
- Different cluster sizes and schedules
- Multiple file formats
- Production-ready autoloader options

### `upload_to_adls.sh`
Script to upload sample data to your ADLS account

## 🚀 Quick Test Setup

### 1. Generate Parquet Files
```bash
cd sample_data
python generate_parquet.py
```

### 2. Upload Sample Data
```bash
# Edit with your storage details
nano upload_to_adls.sh

# Upload files
./upload_to_adls.sh
```

### 3. Use Test Configuration
```bash
# Copy test config
cp sample_data/test_config_adls.tsv config/ingestion_config.tsv

# Update with your storage account name
sed -i 's/oneenvadls/YOUR_STORAGE_ACCOUNT/g' config/ingestion_config.tsv
```

### 4. Deploy and Test
```bash
databricks bundle deploy --profile dev
```

## 📊 Data Characteristics

| Dataset | Format | Records | Key Features |
|---------|--------|---------|--------------|
| **Customers** | CSV | 10 | Demographics, addresses |
| **Products** | CSV | 10 | Catalog with prices |
| **Orders** | JSON | 10 | Nested structure |
| **Order Items** | JSON | 10 | Line-level detail |

### Data Relationships
- **Customers** → **Orders** (1:many)
- **Orders** → **Order Items** (1:many)  
- **Products** → **Order Items** (1:many)

## 🧪 Testing Scenarios

The sample data supports:

✅ **Multiple File Formats** - CSV, JSON, Parquet  
✅ **Schema Evolution** - Add columns to test evolution  
✅ **Data Quality** - Some records with missing values  
✅ **Incremental Loading** - Copy files incrementally  
✅ **Nested Structures** - JSON with complex schemas

## 📋 ADLS Structure

Organize your test data like this:

```
your-container/
├── astellas/
│   ├── csv/           # CSV files
│   ├── json/          # JSON files  
│   └── parquet/       # Parquet files (not used in current config)
└── checkpoint/        # Auto Loader checkpoints
    ├── customers/
    ├── orders/
    ├── order_items/
    └── products/
```

## 🎯 Expected Results

After deployment, you'll have:
- 4 DLT pipelines processing different data sources
- Data flowing into `vbdemos.adls_bronze.*` tables
- Different cluster configurations (medium, large, serverless)
- Various processing schedules (5min, 10min, 15min)

## 🔧 Generate More Data

```bash
# Create additional test files
python generate_parquet.py

# This creates larger datasets for load testing
```

Happy testing! 🚀