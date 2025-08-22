# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Table: Product Analytics
# MAGIC Creates a simple dummy table for product analytics

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Read from the silver SCD2 product table
silver_df = spark.read.table("vbdemos.adls_silver.products_scd2")

# COMMAND ----------

# Debug: Show available columns in the silver table
print("🔍 Available columns in silver table:")
silver_df.printSchema()
print(f"📊 Total records in silver table: {silver_df.count()}")

# COMMAND ----------

# Create a simple dummy analytics table
analytics_df = silver_df.select(
    # Primary key - no masking needed
    col("product_id").cast("string").alias("product_id"),
    
    # Dummy analytics columns
    lit("Analytics Data").alias("data_type"),
    lit("2024").alias("year"),
    lit("Q1").alias("quarter"),
    
    # Dummy calculated field (using literal values instead of potentially missing columns)
    lit(1000).alias("inventory_value"),
    
    # Timestamp for processing
    current_timestamp().alias("processed_at")
)

# COMMAND ----------

# Write to target table (this will be a simple table, not DLT)
analytics_df.write.mode("overwrite").saveAsTable("vbdemos.adls_gold.product_analytics_gold")

# COMMAND ----------

print("✅ Gold table created successfully!")
print(f"📊 Records processed: {analytics_df.count()}")
print(f"🎯 Target table: vbdemos.adls_gold.product_analytics_gold")
