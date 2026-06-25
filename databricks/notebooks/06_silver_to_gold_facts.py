# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 06_silver_to_gold_facts
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook creates the Gold fact tables for the Az-SupplyChain analytical model.
# MAGIC
# MAGIC The facts are built from trusted Silver tables and are designed to support Synapse views, Power BI reporting and later optimization comparison.
# MAGIC
# MAGIC ## Inputs
# MAGIC
# MAGIC Silver tables:
# MAGIC
# MAGIC - `az_supplychain.silver.order_list`
# MAGIC - `az_supplychain.silver.valid_shipping_options`
# MAGIC - `az_supplychain.silver.wh_capacities`
# MAGIC
# MAGIC Gold dimensions expected from notebook `05_silver_to_gold_dimensions`:
# MAGIC
# MAGIC - `az_supplychain.gold.dim_date`
# MAGIC - `az_supplychain.gold.dim_product`
# MAGIC - `az_supplychain.gold.dim_customer`
# MAGIC - `az_supplychain.gold.dim_plant`
# MAGIC - `az_supplychain.gold.dim_port`
# MAGIC - `az_supplychain.gold.dim_carrier`
# MAGIC - `az_supplychain.gold.dim_transport_mode`
# MAGIC - `az_supplychain.gold.dim_service_level`
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC Gold fact tables:
# MAGIC
# MAGIC - `az_supplychain.gold.fact_order_shipment`
# MAGIC - `az_supplychain.gold.fact_freight_cost_option`
# MAGIC - `az_supplychain.gold.fact_warehouse_capacity_usage`
# MAGIC
# MAGIC ## Processing flow
# MAGIC
# MAGIC 1. Load shared project configuration from `00_config`.
# MAGIC 2. Read the required Silver tables.
# MAGIC 3. Build the historical shipment fact.
# MAGIC 4. Build the freight cost option fact.
# MAGIC 5. Build the warehouse order capacity usage fact.
# MAGIC 6. Validate fact keys and measures.
# MAGIC 7. Write Gold facts as external Delta tables.
# MAGIC 8. Validate the written Gold fact tables.
# MAGIC
# MAGIC ## Important notes
# MAGIC
# MAGIC - Gold facts do not carry Bronze or Silver technical metadata.
# MAGIC - `order_id` is handled as a degenerate dimension inside the facts.
# MAGIC - Warehouse capacity is measured as number of orders per day, not units and not weight.
# MAGIC - Warehouse cost is measured per unit and still uses `unit_quant`.
# MAGIC - Freight cost is based on weight and freight rate rules.
# MAGIC - Table names and physical paths come from `00_config`.
# MAGIC - Optimization is not applied in this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load project configuration
# MAGIC
# MAGIC This step imports the shared configuration from `00_config`.
# MAGIC
# MAGIC The configuration contains Unity Catalog names, Silver table names, Gold table names and physical ADLS paths.

# COMMAND ----------

# DBTITLE 1,Load project configuration
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Import PySpark functions
# MAGIC
# MAGIC This step imports the PySpark functions needed to build the Gold facts.
# MAGIC
# MAGIC The main functions used are:
# MAGIC
# MAGIC - `col()` to reference columns clearly.
# MAGIC - `date_format()` to create `date_key`.
# MAGIC - `sum()`, `count()` and `round()` to build aggregated capacity usage metrics.
# MAGIC - `when()` to create the capacity exceeded flag.
# MAGIC - `Window` and `row_number()` to select one historical cost estimate when multiple historical matches exist.

# COMMAND ----------

# DBTITLE 1,Import PySpark functions
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Read required Silver tables
# MAGIC
# MAGIC This step reads the Silver tables needed to build the Gold facts.
# MAGIC
# MAGIC `spark.table()` is used because the tables are already registered in Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Read required Silver tables
silver_orders_df = spark.table(SILVER_TABLES["order_list"])
valid_shipping_options_df = spark.table(SILVER_TABLES["valid_shipping_options"])
silver_wh_capacities_df = spark.table(SILVER_TABLES["wh_capacities"])

print(f"Silver orders rows:              {silver_orders_df.count()}")
print(f"Valid shipping options rows:     {valid_shipping_options_df.count()}")
print(f"Silver warehouse capacities rows:{silver_wh_capacities_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Prepare historical cost estimates
# MAGIC
# MAGIC This step extracts the valid shipping options that match the historical route.
# MAGIC
# MAGIC If more than one historical option is found for the same order, the lowest `total_logistics_cost` is selected.
# MAGIC
# MAGIC This keeps `fact_order_shipment` at one row per order while preserving a realistic historical cost estimate.

# COMMAND ----------

# DBTITLE 1,Prepare historical cost estimates
historical_option_window = (
    Window
    .partitionBy("order_id")
    .orderBy(F.col("total_logistics_cost").asc())
)

historical_cost_df = (
    valid_shipping_options_df
    .filter(F.col("is_historical_option") == True)
    .withColumn("historical_cost_rank", F.row_number().over(historical_option_window))
    .filter(F.col("historical_cost_rank") == 1)
    .select(
        "order_id",
        F.col("freight_cost").alias("historical_freight_cost"),
        F.col("warehouse_handling_cost").alias("historical_warehouse_handling_cost"),
        F.col("total_logistics_cost").alias("historical_total_logistics_cost")
    )
)

print(f"Orders with historical cost estimate: {historical_cost_df.count()}")
display(historical_cost_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Build fact_order_shipment
# MAGIC
# MAGIC This step creates the historical shipment fact at order grain.
# MAGIC
# MAGIC The fact keeps the historical shipment route from `silver.order_list` and adds the estimated historical logistics cost when a matching historical option exists.

# COMMAND ----------

# DBTITLE 1,Build fact_order_shipment
fact_order_shipment_df = (
    silver_orders_df
    .select(
        "order_id",
        "order_date",
        "customer",
        "product_id",
        "plant_code",
        "orig_port",
        "dest_port",
        "carrier",
        "service_level",
        "unit_quant",
        "weight",
        "tpt_day_count",
        "ship_ahead_day_count",
        "ship_late_day_count"
    )
    .withColumn(
        "date_key",
        F.date_format(F.col("order_date"), "yyyyMMdd").cast("int")
    )
    .join(
        historical_cost_df,
        on="order_id",
        how="left"
    )
    .select(
        "order_id",
        "date_key",
        "order_date",
        "customer",
        "product_id",
        "plant_code",
        "orig_port",
        "dest_port",
        "carrier",
        "service_level",
        "unit_quant",
        "weight",
        "tpt_day_count",
        "ship_ahead_day_count",
        "ship_late_day_count",
        "historical_freight_cost",
        "historical_warehouse_handling_cost",
        "historical_total_logistics_cost"
    )
)

print(f"fact_order_shipment rows: {fact_order_shipment_df.count()}")
display(fact_order_shipment_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Build fact_freight_cost_option
# MAGIC
# MAGIC This step creates the freight cost option fact from `silver.valid_shipping_options`.
# MAGIC
# MAGIC The grain is one valid logistics option per order.
# MAGIC
# MAGIC This fact allows analysis of all valid alternatives before the optimization layer selects the lowest-cost option.

# COMMAND ----------

# DBTITLE 1,Build fact_freight_cost_option
fact_freight_cost_option_df = (
    valid_shipping_options_df
    .withColumn(
        "date_key",
        F.date_format(F.col("order_date"), "yyyyMMdd").cast("int")
    )
    .select(
        "order_id",
        "date_key",
        "order_date",
        "customer",
        "product_id",
        "dest_port",
        "service_level",
        "unit_quant",
        "weight",
        "historical_plant_code",
        "historical_orig_port",
        "historical_carrier",
        "historical_tpt_day_count",
        "candidate_plant_code",
        "candidate_orig_port",
        "candidate_carrier",
        "candidate_transport_mode",
        "candidate_carrier_type",
        "candidate_tpt_day_count",
        "freight_min_weight_quant",
        "freight_max_weight_quant",
        "freight_min_cost",
        "freight_rate",
        "warehouse_cost_per_unit",
        "warehouse_daily_order_capacity",
        "is_vmi_customer",
        "is_candidate_plant_vmi_restricted",
        "is_historical_option",
        "freight_cost",
        "warehouse_handling_cost",
        "total_logistics_cost"
    )
)

print(f"fact_freight_cost_option rows: {fact_freight_cost_option_df.count()}")

display(fact_freight_cost_option_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Build fact_warehouse_capacity_usage
# MAGIC
# MAGIC This step creates a daily warehouse order capacity usage fact based on the historical shipment plant.
# MAGIC
# MAGIC The grain is one row per `order_date` and `plant_code`.
# MAGIC
# MAGIC Warehouse capacity is measured in number of orders per day.
# MAGIC
# MAGIC Therefore, capacity usage is calculated with `order_count`, not with `unit_quant` and not with `weight`.
# MAGIC
# MAGIC `total_unit_quantity` and `total_weight` are still included as operational volume metrics, but they are not used to calculate capacity usage.

# COMMAND ----------

# DBTITLE 1,Build fact_warehouse_capacity_usage
warehouse_capacity_df = (
    silver_wh_capacities_df
    .select(
        "plant_code",
        "daily_order_capacity"
    )
)

fact_warehouse_capacity_usage_df = (
    silver_orders_df
    .groupBy(
        "order_date",
        "plant_code"
    )
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("unit_quant").alias("total_unit_quantity"),
        F.sum("weight").alias("total_weight")
    )
    .join(
        warehouse_capacity_df,
        on="plant_code",
        how="left"
    )
    .withColumn(
        "date_key",
        F.date_format(F.col("order_date"), "yyyyMMdd").cast("int")
    )
    .withColumn(
        "capacity_usage_ratio",
        F.round(F.col("order_count") / F.col("daily_order_capacity"), 4)
    )
    .withColumn(
        "remaining_order_capacity",
        F.col("daily_order_capacity") - F.col("order_count")
    )
    .withColumn(
        "is_capacity_exceeded",
        F.when(F.col("order_count") > F.col("daily_order_capacity"), True).otherwise(False)
    )
    .select(
        "date_key",
        "order_date",
        "plant_code",
        "daily_order_capacity",
        "order_count",
        "total_unit_quantity",
        "total_weight",
        "capacity_usage_ratio",
        "remaining_order_capacity",
        "is_capacity_exceeded"
    )
)

print(f"fact_warehouse_capacity_usage rows: {fact_warehouse_capacity_usage_df.count()}")

display(
    fact_warehouse_capacity_usage_df
    .orderBy("order_date", "plant_code")
    .limit(50)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Validate fact_order_shipment
# MAGIC
# MAGIC This step validates that `fact_order_shipment` has one row per order and no null keys in required analytical fields.
# MAGIC
# MAGIC This matters because the table represents the historical shipment grain.

# COMMAND ----------

# DBTITLE 1,Validate fact_order_shipment
order_fact_row_count = fact_order_shipment_df.count()

duplicate_order_count = (
    fact_order_shipment_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

required_order_fact_columns = [
    "order_id",
    "date_key",
    "order_date",
    "customer",
    "product_id",
    "plant_code",
    "dest_port",
    "carrier",
    "service_level",
    "unit_quant",
    "weight"
]

order_fact_null_results = []

for column_name in required_order_fact_columns:
    null_count = fact_order_shipment_df.filter(F.col(column_name).isNull()).count()

    order_fact_null_results.append({
        "column_name": column_name,
        "null_count": null_count
    })

order_fact_null_validation_df = spark.createDataFrame(order_fact_null_results)

display(order_fact_null_validation_df)

print(f"fact_order_shipment rows: {order_fact_row_count}")
print(f"Duplicated order_id rows: {duplicate_order_count}")

if duplicate_order_count > 0:
    raise ValueError("fact_order_shipment has duplicated order_id values.")

failed_null_columns = (
    order_fact_null_validation_df
    .filter(F.col("null_count") > 0)
    .collect()
)

if failed_null_columns:
    raise ValueError("fact_order_shipment has null values in required columns.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Validate fact_freight_cost_option
# MAGIC
# MAGIC This step validates that the cost option fact has valid cost measures.
# MAGIC
# MAGIC Each option must have a positive total logistics cost because it represents a valid cost alternative.

# COMMAND ----------

# DBTITLE 1,Validate fact_freight_cost_option
invalid_cost_options_df = (
    fact_freight_cost_option_df
    .filter(
        (F.col("freight_cost").isNull()) |
        (F.col("warehouse_handling_cost").isNull()) |
        (F.col("total_logistics_cost").isNull()) |
        (F.col("freight_cost") < 0) |
        (F.col("warehouse_handling_cost") < 0) |
        (F.col("total_logistics_cost") <= 0)
    )
)

invalid_cost_option_count = invalid_cost_options_df.count()

print(f"Invalid freight cost option rows: {invalid_cost_option_count}")
display(invalid_cost_options_df.limit(50))

if invalid_cost_option_count > 0:
    raise ValueError("Invalid cost values found in fact_freight_cost_option.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Validate fact_warehouse_capacity_usage
# MAGIC
# MAGIC This step validates that warehouse order capacity usage has valid capacity data and valid numeric values.
# MAGIC
# MAGIC The capacity fact is based on order counts.
# MAGIC
# MAGIC A valid record must have:
# MAGIC
# MAGIC - a positive `daily_order_capacity`
# MAGIC - a positive `order_count`
# MAGIC - a non-null `capacity_usage_ratio`
# MAGIC
# MAGIC Capacity can be exceeded historically, so `is_capacity_exceeded = true` is not treated as an error in this notebook.

# COMMAND ----------

# DBTITLE 1,Validate fact_warehouse_capacity_usage
invalid_capacity_usage_df = (
    fact_warehouse_capacity_usage_df
    .filter(
        (F.col("daily_order_capacity").isNull()) |
        (F.col("daily_order_capacity") <= 0) |
        (F.col("order_count").isNull()) |
        (F.col("order_count") <= 0) |
        (F.col("capacity_usage_ratio").isNull())
    )
)

invalid_capacity_usage_count = invalid_capacity_usage_df.count()

print(f"Invalid warehouse capacity usage rows: {invalid_capacity_usage_count}")

display(invalid_capacity_usage_df.limit(50))

if invalid_capacity_usage_count > 0:
    raise ValueError("Invalid records found in fact_warehouse_capacity_usage.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Write Gold fact tables
# MAGIC
# MAGIC This step writes the Gold fact DataFrames as external Delta tables registered in Unity Catalog.
# MAGIC
# MAGIC The logical table names and physical paths come from `00_config`.
# MAGIC
# MAGIC `overwrite` is used because these Gold facts are rebuilt completely from the trusted Silver layer.

# COMMAND ----------

# DBTITLE 1,Write Gold fact tables
gold_facts_to_write = {
    "fact_order_shipment": fact_order_shipment_df,
    "fact_freight_cost_option": fact_freight_cost_option_df,
    "fact_warehouse_capacity_usage": fact_warehouse_capacity_usage_df
}

for fact_name, dataframe in gold_facts_to_write.items():
    target_table = GOLD_TABLES[fact_name]
    target_path = GOLD_TABLE_PATHS[fact_name]

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", target_path)
        .saveAsTable(target_table)
    )

    print(f"Written Gold fact: {target_table}")
    print(f"Physical path: {target_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 - Validate written Gold facts
# MAGIC
# MAGIC This step reads each Gold fact back from Unity Catalog and returns the final row counts.
# MAGIC
# MAGIC This confirms that all fact tables were written and registered correctly.

# COMMAND ----------

# DBTITLE 1,Validate written Gold facts
gold_fact_validation = []

for fact_name in gold_facts_to_write.keys():
    target_table = GOLD_TABLES[fact_name]
    target_path = GOLD_TABLE_PATHS[fact_name]
    row_count = spark.table(target_table).count()

    gold_fact_validation.append({
        "fact_name": fact_name,
        "unity_catalog_table": target_table,
        "physical_path": target_path,
        "row_count": row_count
    })

gold_fact_validation_df = spark.createDataFrame(gold_fact_validation)

display(gold_fact_validation_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 - List tables in the Gold schema
# MAGIC
# MAGIC This final step lists the tables currently available in `az_supplychain.gold`.
# MAGIC
# MAGIC At this point, the Gold schema should contain both dimensions and facts.

# COMMAND ----------

# DBTITLE 1,List tables in the Gold schema
display(spark.sql(f"SHOW TABLES IN {UC_GOLD}"))