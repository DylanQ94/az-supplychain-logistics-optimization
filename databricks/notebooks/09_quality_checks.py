# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 09_quality_checks
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook runs the final data quality checks for the Az-SupplyChain Databricks pipeline.
# MAGIC
# MAGIC It validates consistency across Silver, Gold and Optimization layers before exposing the model to Synapse Serverless SQL and Power BI.
# MAGIC
# MAGIC ## Inputs
# MAGIC
# MAGIC Silver tables:
# MAGIC
# MAGIC - `az_supplychain.silver.order_list`
# MAGIC - `az_supplychain.silver.valid_shipping_options`
# MAGIC
# MAGIC Gold dimension tables:
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
# MAGIC Gold fact tables:
# MAGIC
# MAGIC - `az_supplychain.gold.fact_order_shipment`
# MAGIC - `az_supplychain.gold.fact_freight_cost_option`
# MAGIC - `az_supplychain.gold.fact_warehouse_capacity_usage`
# MAGIC
# MAGIC Optimization tables:
# MAGIC
# MAGIC - `az_supplychain.optimization.fact_optimized_order_assignment`
# MAGIC - `az_supplychain.optimization.fact_optimized_capacity_usage`
# MAGIC - `az_supplychain.optimization.fact_unassigned_order_diagnostic`
# MAGIC - `az_supplychain.optimization.fact_cost_comparison`
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC This notebook does not write new Delta tables.
# MAGIC
# MAGIC It displays validation summaries and raises errors if critical checks fail.
# MAGIC
# MAGIC ## Processing flow
# MAGIC
# MAGIC 1. Load shared project configuration from `00_config`.
# MAGIC 2. Validate that all expected tables exist.
# MAGIC 3. Read Silver, Gold and Optimization tables.
# MAGIC 4. Validate row count continuity.
# MAGIC 5. Validate dimension key uniqueness.
# MAGIC 6. Validate fact-to-dimension relationships.
# MAGIC 7. Validate Gold fact grains.
# MAGIC 8. Validate optimization coverage.
# MAGIC 9. Validate optimization diagnostics.
# MAGIC 10. Validate cost consistency.
# MAGIC 11. Validate warehouse capacity logic.
# MAGIC 12. Display final quality summary.
# MAGIC
# MAGIC ## Important notes
# MAGIC
# MAGIC - No persistent quality logs are written.
# MAGIC - Critical failures stop the notebook.
# MAGIC - Unassigned optimized orders are allowed only if they are explained in `fact_unassigned_order_diagnostic`.
# MAGIC - Warehouse capacity is measured as number of orders per day.
# MAGIC - This is the final Databricks validation before Synapse and Power BI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load project configuration
# MAGIC
# MAGIC This step imports the shared configuration from `00_config`.
# MAGIC
# MAGIC The configuration contains the Unity Catalog table names and the centralized paths used across the project.

# COMMAND ----------

# DBTITLE 1,Load project configuration
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Import PySpark functions
# MAGIC
# MAGIC This step imports the PySpark functions needed for validation.
# MAGIC
# MAGIC The notebook uses joins, aggregations and filters to check consistency across Silver, Gold and Optimization layers.

# COMMAND ----------

# DBTITLE 1,Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Define expected project tables
# MAGIC
# MAGIC This step defines the list of tables that should exist after running the previous notebooks.
# MAGIC
# MAGIC The table names come from `00_config`, so this notebook does not redefine paths or table locations.

# COMMAND ----------

# DBTITLE 1,Define expected project tables
expected_tables = {
    "silver_order_list": SILVER_TABLES["order_list"],
    "silver_valid_shipping_options": SILVER_TABLES["valid_shipping_options"],

    "gold_dim_date": GOLD_TABLES["dim_date"],
    "gold_dim_product": GOLD_TABLES["dim_product"],
    "gold_dim_customer": GOLD_TABLES["dim_customer"],
    "gold_dim_plant": GOLD_TABLES["dim_plant"],
    "gold_dim_port": GOLD_TABLES["dim_port"],
    "gold_dim_carrier": GOLD_TABLES["dim_carrier"],
    "gold_dim_transport_mode": GOLD_TABLES["dim_transport_mode"],
    "gold_dim_service_level": GOLD_TABLES["dim_service_level"],

    "gold_fact_order_shipment": GOLD_TABLES["fact_order_shipment"],
    "gold_fact_freight_cost_option": GOLD_TABLES["fact_freight_cost_option"],
    "gold_fact_warehouse_capacity_usage": GOLD_TABLES["fact_warehouse_capacity_usage"],

    "optimization_fact_optimized_order_assignment": OPTIMIZATION_TABLES["fact_optimized_order_assignment"],
    "optimization_fact_optimized_capacity_usage": OPTIMIZATION_TABLES["fact_optimized_capacity_usage"],
    "optimization_fact_unassigned_order_diagnostic": OPTIMIZATION_TABLES["fact_unassigned_order_diagnostic"],
    "optimization_fact_cost_comparison": OPTIMIZATION_TABLES["fact_cost_comparison"]
}

for table_alias, table_name in expected_tables.items():
    print(f"{table_alias}: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Validate expected tables exist
# MAGIC
# MAGIC This step checks whether every expected table exists in Unity Catalog.
# MAGIC
# MAGIC If any table is missing, the notebook stops because downstream validation would not be reliable.

# COMMAND ----------

# DBTITLE 1,Validate expected tables exist
table_existence_results = []

for table_alias, table_name in expected_tables.items():
    table_exists = spark.catalog.tableExists(table_name)

    table_existence_results.append({
        "table_alias": table_alias,
        "table_name": table_name,
        "table_exists": table_exists
    })

table_existence_df = spark.createDataFrame(table_existence_results)

display(table_existence_df)

missing_tables = (
    table_existence_df
    .filter(F.col("table_exists") == False)
    .collect()
)

if missing_tables:
    raise ValueError("Some expected tables are missing. Review table_existence_df.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Read Silver, Gold and Optimization tables
# MAGIC
# MAGIC This step reads all tables needed for final validation.
# MAGIC
# MAGIC `spark.table()` is used because all tables are registered in Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Read Silver, Gold and Optimization tables
silver_orders_df = spark.table(SILVER_TABLES["order_list"])
valid_shipping_options_df = spark.table(SILVER_TABLES["valid_shipping_options"])

dim_date_df = spark.table(GOLD_TABLES["dim_date"])
dim_product_df = spark.table(GOLD_TABLES["dim_product"])
dim_customer_df = spark.table(GOLD_TABLES["dim_customer"])
dim_plant_df = spark.table(GOLD_TABLES["dim_plant"])
dim_port_df = spark.table(GOLD_TABLES["dim_port"])
dim_carrier_df = spark.table(GOLD_TABLES["dim_carrier"])
dim_transport_mode_df = spark.table(GOLD_TABLES["dim_transport_mode"])
dim_service_level_df = spark.table(GOLD_TABLES["dim_service_level"])

fact_order_shipment_df = spark.table(GOLD_TABLES["fact_order_shipment"])
fact_freight_cost_option_df = spark.table(GOLD_TABLES["fact_freight_cost_option"])
fact_warehouse_capacity_usage_df = spark.table(GOLD_TABLES["fact_warehouse_capacity_usage"])

fact_optimized_order_assignment_df = spark.table(OPTIMIZATION_TABLES["fact_optimized_order_assignment"])
fact_optimized_capacity_usage_df = spark.table(OPTIMIZATION_TABLES["fact_optimized_capacity_usage"])
fact_unassigned_order_diagnostic_df = spark.table(OPTIMIZATION_TABLES["fact_unassigned_order_diagnostic"])
fact_cost_comparison_df = spark.table(OPTIMIZATION_TABLES["fact_cost_comparison"])

print("All required tables were loaded successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Validate row count continuity
# MAGIC
# MAGIC This step compares key row counts across the pipeline.
# MAGIC
# MAGIC The Silver orders table, Gold shipment fact and Optimization assignment fact should have the same order coverage.

# COMMAND ----------

# DBTITLE 1,Validate row count continuity
row_count_validation = [
    {
        "table_name": "silver.order_list",
        "row_count": silver_orders_df.count()
    },
    {
        "table_name": "silver.valid_shipping_options",
        "row_count": valid_shipping_options_df.count()
    },
    {
        "table_name": "gold.fact_order_shipment",
        "row_count": fact_order_shipment_df.count()
    },
    {
        "table_name": "gold.fact_freight_cost_option",
        "row_count": fact_freight_cost_option_df.count()
    },
    {
        "table_name": "gold.fact_warehouse_capacity_usage",
        "row_count": fact_warehouse_capacity_usage_df.count()
    },
    {
        "table_name": "optimization.fact_optimized_order_assignment",
        "row_count": fact_optimized_order_assignment_df.count()
    },
    {
        "table_name": "optimization.fact_cost_comparison",
        "row_count": fact_cost_comparison_df.count()
    }
]

row_count_validation_df = spark.createDataFrame(row_count_validation)

display(row_count_validation_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Validate dimension key uniqueness
# MAGIC
# MAGIC This step validates that each dimension has unique and non-null keys.
# MAGIC
# MAGIC Dimension keys must be reliable because they are used for Power BI relationships and Synapse analytical joins.

# COMMAND ----------

# DBTITLE 1,Validate dimension key uniqueness
dimension_key_checks = [
    ("dim_date", dim_date_df, "date_key"),
    ("dim_product", dim_product_df, "product_id"),
    ("dim_customer", dim_customer_df, "customer"),
    ("dim_plant", dim_plant_df, "plant_code"),
    ("dim_port", dim_port_df, "port"),
    ("dim_carrier", dim_carrier_df, "carrier"),
    ("dim_transport_mode", dim_transport_mode_df, "transport_mode"),
    ("dim_service_level", dim_service_level_df, "service_level")
]

validation_results = []

for dimension_name, dataframe, key_column in dimension_key_checks:
    total_rows = dataframe.count()

    null_key_count = (
        dataframe
        .filter(F.col(key_column).isNull())
        .count()
    )

    duplicate_key_count = (
        dataframe
        .groupBy(key_column)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    validation_results.append({
        "dimension_name": dimension_name,
        "key_column": key_column,
        "total_rows": total_rows,
        "null_key_count": null_key_count,
        "duplicate_key_count": duplicate_key_count
    })

dimension_validation_df = spark.createDataFrame(validation_results)

display(dimension_validation_df)

failed_dimension_keys = (
    dimension_validation_df
    .filter(
        (F.col("null_key_count") > 0) |
        (F.col("duplicate_key_count") > 0)
    )
    .collect()
)

if failed_dimension_keys:
    raise ValueError("Dimension key validation failed. Review dimension_validation_df.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Validate Gold fact grains
# MAGIC
# MAGIC This step validates the expected grain of the main Gold facts.
# MAGIC
# MAGIC `fact_order_shipment` must have one row per order.
# MAGIC
# MAGIC `fact_warehouse_capacity_usage` must have one row per date and historical plant.

# COMMAND ----------

# DBTITLE 1,Validate Gold fact grains
duplicated_order_fact_count = (
    fact_order_shipment_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

duplicated_warehouse_capacity_usage_count = (
    fact_warehouse_capacity_usage_df
    .groupBy("date_key", "plant_code")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

gold_fact_grain_validation = [
    {
        "fact_name": "fact_order_shipment",
        "grain": "one row per order_id",
        "issue_count": duplicated_order_fact_count
    },
    {
        "fact_name": "fact_warehouse_capacity_usage",
        "grain": "one row per date_key and plant_code",
        "issue_count": duplicated_warehouse_capacity_usage_count
    }
]

gold_fact_grain_validation_df = spark.createDataFrame(gold_fact_grain_validation)

display(gold_fact_grain_validation_df)

failed_gold_fact_grain_checks = (
    gold_fact_grain_validation_df
    .filter(F.col("issue_count") > 0)
    .collect()
)

if failed_gold_fact_grain_checks:
    raise ValueError("Gold fact grain validation failed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Validate cost values in freight options
# MAGIC
# MAGIC This step validates that every logistics option has valid cost values.
# MAGIC
# MAGIC Cost options with null or negative values would affect optimization and savings calculations.

# COMMAND ----------

# DBTITLE 1,Validate cost values in freight options
invalid_freight_cost_options_df = (
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

invalid_freight_cost_option_count = invalid_freight_cost_options_df.count()

print(f"Invalid freight cost option rows: {invalid_freight_cost_option_count}")

display(invalid_freight_cost_options_df.limit(50))

if invalid_freight_cost_option_count > 0:
    raise ValueError("Invalid cost values found in fact_freight_cost_option.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Validate historical warehouse order capacity usage
# MAGIC
# MAGIC This step validates that historical warehouse capacity usage has valid order-capacity values.
# MAGIC
# MAGIC Warehouse capacity is measured by number of orders per day.
# MAGIC
# MAGIC Historical capacity can be exceeded because it represents what happened in the original operation.

# COMMAND ----------

# DBTITLE 1,Validate historical warehouse order capacity usage
invalid_historical_capacity_usage_df = (
    fact_warehouse_capacity_usage_df
    .filter(
        (F.col("daily_order_capacity").isNull()) |
        (F.col("daily_order_capacity") <= 0) |
        (F.col("order_count").isNull()) |
        (F.col("order_count") <= 0) |
        (F.col("capacity_usage_ratio").isNull())
    )
)

invalid_historical_capacity_usage_count = invalid_historical_capacity_usage_df.count()

historical_capacity_exceeded_count = (
    fact_warehouse_capacity_usage_df
    .filter(F.col("is_capacity_exceeded") == True)
    .count()
)

print(f"Invalid historical capacity usage rows: {invalid_historical_capacity_usage_count}")
print(f"Historical capacity exceeded rows:      {historical_capacity_exceeded_count}")

display(invalid_historical_capacity_usage_df.limit(50))
display(
    fact_warehouse_capacity_usage_df
    .filter(F.col("is_capacity_exceeded") == True)
    .limit(50)
)

if invalid_historical_capacity_usage_count > 0:
    raise ValueError("Invalid records found in historical warehouse capacity usage.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Validate optimization coverage
# MAGIC
# MAGIC This step validates that every historical order is either optimized or explained as unassigned.
# MAGIC
# MAGIC With warehouse capacity as a hard constraint, not every order is expected to receive an optimized assignment.
# MAGIC
# MAGIC The correct validation is:
# MAGIC
# MAGIC `historical orders = optimized orders + unassigned diagnostic orders`

# COMMAND ----------

# DBTITLE 1,Validate optimization coverage
historical_order_count = (
    fact_order_shipment_df
    .select("order_id")
    .dropDuplicates()
    .count()
)

optimized_order_count = (
    fact_optimized_order_assignment_df
    .select("order_id")
    .dropDuplicates()
    .count()
)

unassigned_diagnostic_order_count = (
    fact_unassigned_order_diagnostic_df
    .select("order_id")
    .dropDuplicates()
    .count()
)

covered_order_count = optimized_order_count + unassigned_diagnostic_order_count

print(f"Historical orders:              {historical_order_count}")
print(f"Optimized orders:               {optimized_order_count}")
print(f"Unassigned diagnostic orders:   {unassigned_diagnostic_order_count}")
print(f"Covered orders:                 {covered_order_count}")

if covered_order_count != historical_order_count:
    raise ValueError(
        "Optimization coverage validation failed. "
        "Optimized orders plus unassigned diagnostic orders must equal historical orders."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 - Validate no overlap between assigned and unassigned orders
# MAGIC
# MAGIC This step validates that an order cannot be both optimized and unassigned.
# MAGIC
# MAGIC The optimized assignment and unassigned diagnostic tables must be mutually exclusive by `order_id`.

# COMMAND ----------

# DBTITLE 1,Validate no overlap between assigned and unassigned orders
orders_in_both_assignment_and_unassigned_df = (
    fact_optimized_order_assignment_df
    .select("order_id")
    .dropDuplicates()
    .join(
        fact_unassigned_order_diagnostic_df
        .select("order_id")
        .dropDuplicates(),
        on="order_id",
        how="inner"
    )
)

orders_in_both_count = orders_in_both_assignment_and_unassigned_df.count()

print(f"Orders both assigned and unassigned: {orders_in_both_count}")

display(orders_in_both_assignment_and_unassigned_df)

if orders_in_both_count > 0:
    raise ValueError("Some orders appear both assigned and unassigned.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 - Validate optimized assignment grain
# MAGIC
# MAGIC This step validates that the optimized assignment table has at most one row per order.
# MAGIC
# MAGIC This confirms that each optimized order received only one selected logistics option.

# COMMAND ----------

# DBTITLE 1,Validate optimized assignment grain
duplicated_optimized_assignment_count = (
    fact_optimized_order_assignment_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

print(f"Duplicated optimized assignment orders: {duplicated_optimized_assignment_count}")

if duplicated_optimized_assignment_count > 0:
    raise ValueError("Duplicate optimized assignments found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14 - Validate optimized capacity usage
# MAGIC
# MAGIC This step validates that optimized assignments do not exceed warehouse daily order capacity.
# MAGIC
# MAGIC The validation uses the diagnostic table created by notebook `08_optimization_diagnostics`.

# COMMAND ----------

# DBTITLE 1,Validate optimized capacity usage
optimized_capacity_exceeded_df = (
    fact_optimized_capacity_usage_df
    .filter(F.col("is_capacity_exceeded") == True)
)

optimized_capacity_exceeded_count = optimized_capacity_exceeded_df.count()

print(f"Optimized capacity exceeded rows: {optimized_capacity_exceeded_count}")

display(optimized_capacity_exceeded_df)

if optimized_capacity_exceeded_count > 0:
    raise ValueError("Optimized capacity exceeded in diagnostic table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15 - Validate unassignment diagnostic reasons
# MAGIC
# MAGIC This step validates that all unassigned orders have a recognized diagnostic reason.
# MAGIC
# MAGIC `POTENTIAL_GREEDY_ISSUE` and `REVIEW_REQUIRED` are allowed categories, but should be reviewed if they appear.

# COMMAND ----------

# DBTITLE 1,Validate unassignment diagnostic reasons
valid_unassignment_reasons = [
    "CAPACITY_BLOCKED",
    "NO_VALID_OPTIONS_AFTER_NETWORK_CONSTRAINTS",
    "POTENTIAL_GREEDY_ISSUE",
    "REVIEW_REQUIRED"
]

invalid_unassignment_reason_df = (
    fact_unassigned_order_diagnostic_df
    .filter(~F.col("constraint_diagnostic_reason").isin(valid_unassignment_reasons))
)

invalid_unassignment_reason_count = invalid_unassignment_reason_df.count()

potential_greedy_issue_count = (
    fact_unassigned_order_diagnostic_df
    .filter(F.col("constraint_diagnostic_reason") == "POTENTIAL_GREEDY_ISSUE")
    .count()
)

review_required_count = (
    fact_unassigned_order_diagnostic_df
    .filter(F.col("constraint_diagnostic_reason") == "REVIEW_REQUIRED")
    .count()
)

print(f"Invalid unassignment reason rows: {invalid_unassignment_reason_count}")
print(f"Potential greedy issue rows:      {potential_greedy_issue_count}")
print(f"Review required rows:             {review_required_count}")

display(invalid_unassignment_reason_df.limit(50))

if invalid_unassignment_reason_count > 0:
    raise ValueError("Invalid unassignment diagnostic reasons found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 16 - Validate optimization cost calculations
# MAGIC
# MAGIC This step validates savings calculations in the optimized assignment table.
# MAGIC
# MAGIC For records with historical cost, the expected savings must equal:
# MAGIC
# MAGIC `historical_total_cost - optimized_total_cost`

# COMMAND ----------

# DBTITLE 1,Validate optimization cost calculations
optimization_cost_validation_df = (
    fact_optimized_order_assignment_df
    .filter(F.col("historical_total_cost").isNotNull())
    .withColumn(
        "expected_estimated_savings",
        F.col("historical_total_cost") - F.col("optimized_total_cost")
    )
    .withColumn(
        "savings_difference",
        F.round(F.col("estimated_savings") - F.col("expected_estimated_savings"), 6)
    )
    .filter(F.col("savings_difference") != 0)
)

optimization_cost_validation_count = optimization_cost_validation_df.count()

print(f"Optimization savings mismatch rows: {optimization_cost_validation_count}")

display(optimization_cost_validation_df.limit(50))

if optimization_cost_validation_count > 0:
    raise ValueError("Savings calculation mismatch found in optimization assignment.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 17 - Validate cost comparison summary
# MAGIC
# MAGIC This step validates that `fact_cost_comparison` matches the detailed optimization outputs.
# MAGIC
# MAGIC The summary must match:
# MAGIC
# MAGIC - historical order count
# MAGIC - optimized order count
# MAGIC - unassigned diagnostic order count
# MAGIC - compared cost totals

# COMMAND ----------

# DBTITLE 1,Validate cost comparison summary
cost_comparison_row_count = fact_cost_comparison_df.count()

if cost_comparison_row_count != 1:
    raise ValueError("fact_cost_comparison must have exactly one row.")

comparison_row = fact_cost_comparison_df.collect()[0]

if comparison_row["historical_order_count"] != historical_order_count:
    raise ValueError("Cost comparison historical_order_count does not match fact_order_shipment.")

if comparison_row["orders_with_optimized_assignment"] != optimized_order_count:
    raise ValueError("Cost comparison optimized order count does not match optimized assignment fact.")

if comparison_row["orders_unassigned_under_constraints"] != unassigned_diagnostic_order_count:
    raise ValueError("Cost comparison unassigned order count does not match unassigned diagnostic fact.")

comparison_source_df = (
    fact_optimized_order_assignment_df
    .filter(F.col("historical_total_cost").isNotNull())
    .agg(
        F.count("order_id").alias("expected_orders_compared"),
        F.sum("historical_total_cost").alias("expected_historical_total_cost"),
        F.sum("optimized_total_cost").alias("expected_optimized_total_cost")
    )
    .withColumn(
        "expected_estimated_savings",
        F.col("expected_historical_total_cost") - F.col("expected_optimized_total_cost")
    )
)

comparison_validation_df = (
    fact_cost_comparison_df
    .crossJoin(comparison_source_df)
    .withColumn(
        "orders_compared_match",
        F.col("orders_compared") == F.col("expected_orders_compared")
    )
    .withColumn(
        "historical_total_cost_difference",
        F.round(F.col("historical_total_cost") - F.col("expected_historical_total_cost"), 6)
    )
    .withColumn(
        "optimized_total_cost_difference",
        F.round(F.col("optimized_total_cost") - F.col("expected_optimized_total_cost"), 6)
    )
    .withColumn(
        "estimated_savings_difference",
        F.round(F.col("estimated_savings") - F.col("expected_estimated_savings"), 6)
    )
)

display(comparison_validation_df)

failed_cost_comparison_count = (
    comparison_validation_df
    .filter(
        (F.col("orders_compared_match") == False) |
        (F.col("historical_total_cost_difference") != 0) |
        (F.col("optimized_total_cost_difference") != 0) |
        (F.col("estimated_savings_difference") != 0)
    )
    .count()
)

if failed_cost_comparison_count > 0:
    raise ValueError("fact_cost_comparison does not match detailed optimization results.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 18 - Create final quality summary
# MAGIC
# MAGIC This step creates a final quality summary.
# MAGIC
# MAGIC The summary is displayed in the notebook but is not written as a persistent Delta table.

# COMMAND ----------

# DBTITLE 1,Create final quality summary
quality_summary = [
    {
        "quality_area": "table_existence",
        "check_name": "all_expected_tables_exist",
        "status": "PASS",
        "issue_count": 0
    },
    {
        "quality_area": "dimension_keys",
        "check_name": "dimension_keys_unique_and_not_null",
        "status": "PASS" if len(failed_dimension_keys) == 0 else "FAIL",
        "issue_count": len(failed_dimension_keys)
    },
    {
        "quality_area": "gold_fact_grain",
        "check_name": "gold_fact_grains_valid",
        "status": "PASS" if len(failed_gold_fact_grain_checks) == 0 else "FAIL",
        "issue_count": len(failed_gold_fact_grain_checks)
    },
    {
        "quality_area": "costs",
        "check_name": "freight_option_costs_valid",
        "status": "PASS" if invalid_freight_cost_option_count == 0 else "FAIL",
        "issue_count": invalid_freight_cost_option_count
    },
    {
        "quality_area": "capacity",
        "check_name": "historical_capacity_values_valid",
        "status": "PASS" if invalid_historical_capacity_usage_count == 0 else "FAIL",
        "issue_count": invalid_historical_capacity_usage_count
    },
    {
        "quality_area": "optimization_coverage",
        "check_name": "optimized_plus_unassigned_equals_historical_orders",
        "status": "PASS" if covered_order_count == historical_order_count else "FAIL",
        "issue_count": abs(historical_order_count - covered_order_count)
    },
    {
        "quality_area": "optimization_coverage",
        "check_name": "no_overlap_between_assigned_and_unassigned_orders",
        "status": "PASS" if orders_in_both_count == 0 else "FAIL",
        "issue_count": orders_in_both_count
    },
    {
        "quality_area": "optimization_grain",
        "check_name": "one_optimized_assignment_per_order",
        "status": "PASS" if duplicated_optimized_assignment_count == 0 else "FAIL",
        "issue_count": duplicated_optimized_assignment_count
    },
    {
        "quality_area": "optimization_capacity",
        "check_name": "optimized_capacity_not_exceeded",
        "status": "PASS" if optimized_capacity_exceeded_count == 0 else "FAIL",
        "issue_count": optimized_capacity_exceeded_count
    },
    {
        "quality_area": "optimization_diagnostics",
        "check_name": "valid_unassignment_reasons",
        "status": "PASS" if invalid_unassignment_reason_count == 0 else "FAIL",
        "issue_count": invalid_unassignment_reason_count
    },
    {
        "quality_area": "optimization_costs",
        "check_name": "optimization_savings_consistent",
        "status": "PASS" if optimization_cost_validation_count == 0 else "FAIL",
        "issue_count": optimization_cost_validation_count
    },
    {
        "quality_area": "optimization_costs",
        "check_name": "cost_comparison_matches_detail",
        "status": "PASS" if failed_cost_comparison_count == 0 else "FAIL",
        "issue_count": failed_cost_comparison_count
    },
    {
        "quality_area": "informational",
        "check_name": "historical_capacity_exceeded_rows",
        "status": "INFO",
        "issue_count": historical_capacity_exceeded_count
    },
    {
        "quality_area": "informational",
        "check_name": "potential_greedy_issue_rows",
        "status": "INFO",
        "issue_count": potential_greedy_issue_count
    },
    {
        "quality_area": "informational",
        "check_name": "review_required_unassignment_rows",
        "status": "INFO",
        "issue_count": review_required_count
    }
]

quality_summary_df = spark.createDataFrame(quality_summary)

display(quality_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 19 - Confirm final Databricks pipeline status
# MAGIC
# MAGIC This step confirms whether all critical quality checks passed.
# MAGIC
# MAGIC If this cell runs successfully, the Databricks pipeline is ready for the next project phase.

# COMMAND ----------

# DBTITLE 1,Confirm final Databricks pipeline status
failed_quality_checks = (
    quality_summary_df
    .filter(F.col("status") == "FAIL")
    .count()
)

if failed_quality_checks > 0:
    raise ValueError("Final quality checks failed. Review quality_summary_df.")

print("All critical Databricks quality checks passed.")
print("Az-SupplyChain Databricks pipeline is ready for Synapse and Power BI phases.")