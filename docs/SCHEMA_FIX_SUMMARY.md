# Schema Fix for Direct Publish Mode

## Problem Description

The silver tables were being created in the **bronze schema** (`vbdemos.adls_bronze.orders_scd2`) instead of the **silver schema** (`vbdemos.adls_silver.orders_scd2`). This happened because the pipeline generator was hardcoded to use `schema="adls_bronze"` for all pipelines.

## Root Cause

In `resources/unified_pipeline_generator.py`, the pipeline creation was hardcoded:

```python
# BEFORE (Incorrect)
pipeline = Pipeline(
    name=f"unified_{pipeline_group}",
    # ... other config ...
    schema="adls_bronze",  # ❌ Hardcoded to bronze schema
    # ... other config ...
)
```

This meant that even though the TSV config specified:
- **Source**: `vbdemos.adls_bronze.orders_new` (bronze table)
- **Target**: `vbdemos.adls_silver.orders_scd2` (silver table)

The pipeline was still using the bronze schema, causing silver tables to be created in the wrong location.

## Solution Applied

### 1. **Dynamic Schema Detection**
Updated the pipeline generator to automatically detect the correct target schema from the silver table configuration:

```python
# AFTER (Correct)
# Determine the correct schema for this pipeline (should be silver schema)
target_schema = "adls_bronze"  # Default fallback
for row in group_rows:
    if row.get('operation_type') == 'silver':
        target_table = row.get('target_table', '')
        if target_table and '.' in target_table:
            # Extract schema from target_table (e.g., vbdemos.adls_silver.orders_scd2)
            parts = target_table.split('.')
            if len(parts) >= 2:
                target_schema = parts[1]  # Get the schema part
                print(f"   🎯 Using target schema: {target_schema} (from {target_table})")
                break

pipeline = Pipeline(
    name=f"unified_{pipeline_group}",
    # ... other config ...
    schema=target_schema,  # ✅ Dynamic schema (silver, not bronze)
    # ... other config ...
)
```

### 2. **Schema Extraction Logic**
The system now:
- ✅ **Reads the silver table target** from the TSV config
- ✅ **Extracts the schema part** (e.g., `adls_silver` from `vbdemos.adls_silver.orders_scd2`)
- ✅ **Applies the correct schema** to the pipeline definition
- ✅ **Falls back to bronze** only if no silver table is found

## Current Status - All Pipelines Now Use Correct Schema

| Pipeline | ✅ **Target Schema** | ✅ **Source** | ✅ **Target** |
|----------|---------------------|---------------|---------------|
| **Customer** | `adls_silver` | `vbdemos.adls_bronze.customers_new` | `vbdemos.adls_silver.customers_scd2` |
| **Order** | `adls_silver` | `vbdemos.adls_bronze.orders_new` | `vbdemos.adls_silver.orders_scd2` |
| **Order Items** | `adls_silver` | `vbdemos.adls_bronze.order_items_new` | `vbdemos.adls_silver.order_items_scd2` |
| **Product** | `adls_silver` | `vbdemos.adls_bronze.products_new` | `vbdemos.adls_silver.products_scd2` |

## Verification Output

The pipeline generator now correctly identifies and applies the target schema:

```
🎯 Using target schema: adls_silver (from vbdemos.adls_silver.customers_scd2)
🎯 Using target schema: adls_silver (from vbdemos.adls_silver.orders_scd2)
🎯 Using target schema: adls_silver (from vbdemos.adls_silver.order_items_scd2)
🎯 Using target schema: adls_silver (from vbdemos.adls_silver.products_scd2)
```

## Why This Matters for Direct Publish Mode

### **Before (Incorrect)**
- ❌ Silver tables created in `vbdemos.adls_bronze.*` schema
- ❌ Wrong data location for downstream consumers
- ❌ Schema confusion and potential data access issues

### **After (Correct)**
- ✅ Silver tables created in `vbdemos.adls_silver.*` schema
- ✅ Proper data lake architecture (bronze → silver → gold)
- ✅ Correct schema isolation for different data quality levels
- ✅ Proper access control and governance

## Complete Configuration Now Working

Each pipeline now has:
1. ✅ **Correct target schema** (silver, not bronze)
2. ✅ **Complete SCD2 configuration** from TSV file
3. ✅ **Proper table paths** for both source and target
4. ✅ **Serverless compute** for quick testing
5. ✅ **Scheduled execution** based on cron expressions

## Result

- ✅ **Silver tables will now be created** in the correct `adls_silver` schema
- ✅ **Direct publish mode** will work correctly with proper schema separation
- ✅ **Data lake architecture** is properly maintained
- ✅ **No more schema confusion** between bronze and silver layers

Your pipelines are now ready for proper deployment with correct schema separation! 🎉
