#!/bin/bash
# Script to upload sample data files to ADLS for testing the autoloader framework

echo "🚀 Uploading sample data files to ADLS..."

# Configuration - UPDATED FOR YOUR ENVIRONMENT
STORAGE_ACCOUNT="oneenvadls"
CONTAINER_NAME="aaboode"
RESOURCE_GROUP="your_resource_group"  # Update this if needed
BASE_DIR="astellas"

# Check if Azure CLI is installed
if ! command -v az &> /dev/null; then
    echo "❌ Azure CLI is not installed. Please install it first."
    exit 1
fi

# Check if logged in to Azure
if ! az account show &> /dev/null; then
    echo "❌ Not logged in to Azure. Please run 'az login' first."
    exit 1
fi

echo "✅ Azure CLI is available and logged in"
echo "📦 Storage Account: $STORAGE_ACCOUNT"
echo "📦 Container: $CONTAINER_NAME"
echo "📁 Base Directory: $BASE_DIR"

# Create container if it doesn't exist
echo "📦 Creating container '$CONTAINER_NAME' if it doesn't exist..."
az storage container create \
    --name "$CONTAINER_NAME" \
    --account-name "$STORAGE_ACCOUNT" \
    --auth-mode login

# Upload CSV files to astellas/csv subdirectory
echo "📤 Uploading CSV files to $BASE_DIR/csv/..."
az storage blob upload-batch \
    --source . \
    --destination "$CONTAINER_NAME/$BASE_DIR/csv" \
    --pattern "*.csv" \
    --account-name "$STORAGE_ACCOUNT" \
    --auth-mode login

# Upload JSON files to astellas/json subdirectory
echo "📤 Uploading JSON files to $BASE_DIR/json/..."
az storage blob upload-batch \
    --source . \
    --destination "$CONTAINER_NAME/$BASE_DIR/json" \
    --pattern "*.json" \
    --account-name "$STORAGE_ACCOUNT" \
    --auth-mode login

# Upload Parquet files to astellas/parquet subdirectory
echo "📤 Uploading Parquet files to $BASE_DIR/parquet/..."
az storage blob upload-batch \
    --source . \
    --destination "$CONTAINER_NAME/$BASE_DIR/parquet" \
    --pattern "*.parquet" \
    --account-name "$STORAGE_ACCOUNT" \
    --auth-mode login

echo "✅ Upload completed successfully!"
echo ""
echo "📁 Files uploaded to:"
echo "  - CSV: abfss://$CONTAINER_NAME@$STORAGE_ACCOUNT.dfs.core.windows.net/$BASE_DIR/csv/"
echo "  - JSON: abfss://$CONTAINER_NAME@$STORAGE_ACCOUNT.dfs.core.windows.net/$BASE_DIR/json/"
echo "  - Parquet: abfss://$CONTAINER_NAME@$STORAGE_ACCOUNT.dfs.core.windows.net/$BASE_DIR/parquet/"
echo ""
echo "🔧 Update your ingestion_config.tsv with these paths to test the framework!"
echo ""
echo "📋 Example paths for your config:"
echo "  - Customers: abfss://$CONTAINER_NAME@$STORAGE_ACCOUNT.dfs.core.windows.net/$BASE_DIR/csv/customers.csv"
echo "  - Orders: abfss://$CONTAINER_NAME@$STORAGE_ACCOUNT.dfs.core.windows.net/$BASE_DIR/json/orders.json"
echo "  - Products: abfss://$CONTAINER_NAME@$STORAGE_ACCOUNT.dfs.core.windows.net/$BASE_DIR/parquet/products.parquet"
