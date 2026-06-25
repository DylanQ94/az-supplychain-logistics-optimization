# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02_bronze_to_silver_orders
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook transforms the Bronze `order_list` table into a clean and typed Silver `order_list` table.
# MAGIC
# MAGIC The Silver orders table is the trusted version of the historical shipment orders. It will be used later to build valid shipping options, Gold fact tables and the optimization layer.
# MAGIC
# MAGIC ## Inputs
# MAGIC
# MAGIC - `az_supplychain.bronze.order_list`
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC - `az_supplychain.silver.order_list`
# MAGIC
# MAGIC ## Processing flow
# MAGIC
# MAGIC 1. Load shared project configuration from `00_config`.
# MAGIC 2. Read the Bronze order list table.
# MAGIC 3. Select the expected columns explicitly.
# MAGIC 4. Standardize business identifier columns.
# MAGIC 5. Cast dates and numeric columns to the correct types.
# MAGIC 6. Validate nulls, duplicated orders and invalid numeric values.
# MAGIC 7. Write the clean Silver orders table as an external Delta table.
# MAGIC 8. Validate the final Silver table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load project configuration
# MAGIC
# MAGIC This step imports the shared configuration from `00_config`.
# MAGIC
# MAGIC The configuration contains the Unity Catalog table names and the physical ADLS paths where Silver Delta tables will be stored.

# COMMAND ----------

# DBTITLE 1,Load project configuration
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Read the Bronze order list table
# MAGIC
# MAGIC This step reads the Bronze `order_list` table from Unity Catalog.
# MAGIC
# MAGIC `spark.table()` is used because the table is already registered in Unity Catalog. This keeps the code independent from the physical ADLS path and uses the governed table name directly.

# COMMAND ----------

# DBTITLE 1,Read the Bronze order list table
bronze_orders_df = spark.table(BRONZE_TABLES["order_list"])

print(f"Bronze orders row count: {bronze_orders_df.count()}")
display(bronze_orders_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Select the expected Bronze columns
# MAGIC
# MAGIC This step selects only the columns needed for the Silver orders table.
# MAGIC
# MAGIC The goal is to make the schema explicit and avoid carrying unexpected columns into Silver.

# COMMAND ----------

# DBTITLE 1,Select the expected Bronze columns
orders_selected_df = bronze_orders_df.select(
    "order_id",
    "order_date",
    "orig_port",
    "carrier",
    "tpt_day_count",
    "service_level",
    "ship_ahead_day_count",
    "ship_late_day_count",
    "customer",
    "product_id",
    "plant_code",
    "dest_port",
    "unit_quant",
    "weight",
    "source_file_path",
    "ingestion_timestamp"
)

display(orders_selected_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Standardize business identifier columns
# MAGIC
# MAGIC This step applies `trim()` and `upper()` to business identifiers and logistics codes.
# MAGIC
# MAGIC This is done in Silver because these columns will be used later in joins with freight rates, plant-port constraints, product-plant compatibility and VMI customer rules.

# COMMAND ----------

# DBTITLE 1,Standardize business identifier columns
from pyspark.sql import functions as F

orders_standardized_df = (
    orders_selected_df
    .withColumn("order_id", F.trim(F.col("order_id").cast("string")))
    .withColumn("orig_port", F.upper(F.trim(F.col("orig_port").cast("string"))))
    .withColumn("carrier", F.upper(F.trim(F.col("carrier").cast("string"))))
    .withColumn("service_level", F.upper(F.trim(F.col("service_level").cast("string"))))
    .withColumn("customer", F.upper(F.trim(F.col("customer").cast("string"))))
    .withColumn("product_id", F.upper(F.trim(F.col("product_id").cast("string"))))
    .withColumn("plant_code", F.upper(F.trim(F.col("plant_code").cast("string"))))
    .withColumn("dest_port", F.upper(F.trim(F.col("dest_port").cast("string"))))
)

display(orders_standardized_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Cast date and numeric columns
# MAGIC
# MAGIC This step converts the order date and numeric measures to their expected types.
# MAGIC
# MAGIC `to_date()` is used for `order_date` because the column will support time-based analysis.
# MAGIC
# MAGIC `cast()` is used for quantities, weights and day-count columns because these fields will be used later in calculations, validations and optimization logic.

# COMMAND ----------

# DBTITLE 1,Cast date and numeric columns
orders_typed_df = (
    orders_standardized_df
    .withColumn("order_date", F.to_date(F.col("order_date")))
    .withColumn("tpt_day_count", F.col("tpt_day_count").cast("int"))
    .withColumn("ship_ahead_day_count", F.col("ship_ahead_day_count").cast("int"))
    .withColumn("ship_late_day_count", F.col("ship_late_day_count").cast("int"))
    .withColumn("unit_quant", F.col("unit_quant").cast("int"))
    .withColumn("weight", F.col("weight").cast("double"))
)

orders_typed_df.printSchema()
display(orders_typed_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Validate required fields
# MAGIC
# MAGIC This step checks nulls in the fields required to identify an order, connect it with logistics constraints and calculate future costs.

# COMMAND ----------

# DBTITLE 1,Validate required fields
required_columns = [
    "order_id",
    "order_date",
    "carrier",
    "service_level",
    "customer",
    "product_id",
    "plant_code",
    "dest_port",
    "unit_quant",
    "weight"
]

null_validation_rows = []

for column_name in required_columns:
    null_count = orders_typed_df.filter(F.col(column_name).isNull()).count()
    null_validation_rows.append((column_name, null_count))

null_validation_df = spark.createDataFrame(
    null_validation_rows,
    ["column_name", "null_count"]
)

display(null_validation_df)

columns_with_nulls = [
    row["column_name"]
    for row in null_validation_df.collect()
    if row["null_count"] > 0
]

if columns_with_nulls:
    print(f"Required columns contain null values: {columns_with_nulls}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Validate duplicated order IDs
# MAGIC
# MAGIC This step validates whether `order_id` is unique.
# MAGIC
# MAGIC A duplicated order would distort shipment counts, total weight, total quantity, warehouse capacity usage and optimization results.

# COMMAND ----------

# DBTITLE 1,Validate duplicated order IDs
duplicate_orders_df = (
    orders_typed_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_order_count = duplicate_orders_df.count()

display(duplicate_orders_df)

if duplicate_order_count > 0:
    print(f"Duplicated order_id values found: {duplicate_order_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Validate numeric business rules
# MAGIC
# MAGIC This step validates simple numeric rules that must be true for the orders table.
# MAGIC
# MAGIC - `unit_quant` must be greater than zero.
# MAGIC - `weight` must be greater than zero.
# MAGIC - Day-count columns should not be negative.
# MAGIC
# MAGIC These validations prevent invalid records from moving into Silver.

# COMMAND ----------

# DBTITLE 1,Validate numeric business rules
invalid_numeric_orders_df = orders_typed_df.filter(
    (F.col("unit_quant") <= 0) |
    (F.col("weight") <= 0) |
    (F.col("tpt_day_count") < 0) |
    (F.col("ship_ahead_day_count") < 0) |
    (F.col("ship_late_day_count") < 0)
)

valid_orders_df = orders_typed_df.filter(
    (F.col("unit_quant") > 0) &
    (F.col("weight") > 0) &
    (F.col("tpt_day_count") >= 0) &
    (F.col("ship_ahead_day_count") >= 0) &
    (F.col("ship_late_day_count") >= 0)
)

invalid_numeric_count = invalid_numeric_orders_df.count()
valid_orders_count = valid_orders_df.count()

print(f"Invalid numeric order records: {invalid_numeric_count}")
print(f"Valid order records:           {valid_orders_count}")

display(invalid_numeric_orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Create the final Silver orders DataFrame
# MAGIC
# MAGIC This step defines the final Silver schema in a controlled order.
# MAGIC
# MAGIC No extra columns are created. The output keeps only business columns and the minimal traceability metadata inherited from Bronze.

# COMMAND ----------

# DBTITLE 1,Create the final Silver orders DataFrame
silver_orders_df = valid_orders_df.select(
    "order_id",
    "order_date",
    "orig_port",
    "carrier",
    "tpt_day_count",
    "service_level",
    "ship_ahead_day_count",
    "ship_late_day_count",
    "customer",
    "product_id",
    "plant_code",
    "dest_port",
    "unit_quant",
    "weight",
    "source_file_path",
    "ingestion_timestamp"
)

# Fixing the native float type of the order_id column.
silver_orders_df = (
    silver_orders_df
    .withColumn(
        "order_id",
        F.round(F.col("order_id").cast("double"), 0)
         .cast("long")
         .cast("string")
    )
)

print(f"Silver orders row count: {silver_orders_df.count()}")
silver_orders_df.printSchema()
display(silver_orders_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Write the Silver orders table
# MAGIC
# MAGIC This step writes the final Silver orders DataFrame as an external Delta table.
# MAGIC
# MAGIC The logical table is registered in Unity Catalog as `az_supplychain.silver.order_list`.
# MAGIC
# MAGIC The physical data is stored in ADLS under the Silver layer.
# MAGIC
# MAGIC `overwrite` is used because this dataset is static and the Silver table is rebuilt completely from Bronze during reprocessing.

# COMMAND ----------

# DBTITLE 1,Write the Silver orders table
target_table = SILVER_TABLES["order_list"]
target_path = SILVER_TABLE_PATHS["order_list"]

(
    silver_orders_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("path", target_path)
    .saveAsTable(target_table)
)

print(f"Written Silver table: {target_table}")
print(f"Physical path: {target_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Validate the Silver orders table
# MAGIC
# MAGIC This step reads the table back from Unity Catalog and validates the final record count.
# MAGIC
# MAGIC This confirms that the table was correctly registered and can be queried by downstream notebooks.

# COMMAND ----------

# DBTITLE 1,Validate the Silver orders table
silver_orders_table_df = spark.table(SILVER_TABLES["order_list"])

print(f"Final Silver orders row count: {silver_orders_table_df.count()}")

display(silver_orders_table_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 - Review the Delta table location
# MAGIC
# MAGIC This step displays the Unity Catalog table metadata.
# MAGIC
# MAGIC The goal is to confirm that the logical table points to the expected physical ADLS location.

# COMMAND ----------

# DBTITLE 1,Review the Delta table location
display(spark.sql(f"DESCRIBE DETAIL {SILVER_TABLES['order_list']}"))