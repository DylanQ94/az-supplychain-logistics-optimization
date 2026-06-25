# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 07_optimization_model
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook creates the Optimization layer for the Az-SupplyChain project.
# MAGIC
# MAGIC It selects one optimized logistics option per order and compares the estimated historical cost against the optimized cost.
# MAGIC
# MAGIC ## Inputs
# MAGIC
# MAGIC Gold tables:
# MAGIC
# MAGIC - `az_supplychain.gold.fact_order_shipment`
# MAGIC - `az_supplychain.gold.fact_freight_cost_option`
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC Optimization tables:
# MAGIC
# MAGIC - `az_supplychain.optimization.fact_optimized_order_assignment`
# MAGIC - `az_supplychain.optimization.fact_cost_comparison`
# MAGIC
# MAGIC ## Processing flow
# MAGIC
# MAGIC 1. Load shared project configuration from `00_config`.
# MAGIC 2. Read the Gold fact tables.
# MAGIC 3. Prepare valid cost options for optimization.
# MAGIC 4. Rank options by cost per order.
# MAGIC 5. Apply a simple capacity-aware greedy assignment.
# MAGIC 6. Create the optimized assignment fact.
# MAGIC 7. Create the cost comparison summary fact.
# MAGIC 8. Validate optimized assignments and cost metrics.
# MAGIC 9. Write Optimization tables as external Delta tables.
# MAGIC 10. Validate the written Optimization tables.
# MAGIC
# MAGIC ## Important notes
# MAGIC
# MAGIC - This notebook does not create new logistics options.
# MAGIC - The optimizer only selects from valid options already created in Silver and exposed in Gold.
# MAGIC - The service level from the original order is preserved.
# MAGIC - Capacity is respected using a simple greedy allocation.
# MAGIC - Savings are calculated only when historical cost is available.
# MAGIC - Table names and physical paths come from `00_config`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load project configuration
# MAGIC
# MAGIC This step imports the shared configuration from `00_config`.
# MAGIC
# MAGIC The configuration contains Unity Catalog names, Gold table names and Optimization table paths.

# COMMAND ----------

# DBTITLE 1,Load project configuration
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Import required libraries
# MAGIC
# MAGIC This step imports PySpark functions for ranking, validation and cost calculations.
# MAGIC
# MAGIC Pandas is used only for the greedy capacity-aware assignment because this first optimization version prioritizes clarity and explainability over solver complexity.

# COMMAND ----------

# DBTITLE 1,Import required libraries
import pandas as pd

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Read Gold fact and dimension tables
# MAGIC
# MAGIC This step reads the Gold tables needed for optimization and diagnostics.
# MAGIC
# MAGIC `fact_freight_cost_option` contains all valid options per order.
# MAGIC
# MAGIC `fact_order_shipment` contains the historical shipment and historical cost estimate.
# MAGIC
# MAGIC `dim_plant` contains the daily order capacity per plant.

# COMMAND ----------

# DBTITLE 1,Read Gold fact tables
fact_order_shipment_df = spark.table(GOLD_TABLES["fact_order_shipment"])
fact_freight_cost_option_df = spark.table(GOLD_TABLES["fact_freight_cost_option"])
dim_plant_df = spark.table(GOLD_TABLES["dim_plant"])

print(f"Gold fact_order_shipment rows:       {fact_order_shipment_df.count()}")
print(f"Gold fact_freight_cost_option rows:  {fact_freight_cost_option_df.count()}")
print(f"Gold dim_plant rows:                 {dim_plant_df.count()}")

display(fact_freight_cost_option_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Prepare valid options for optimization
# MAGIC
# MAGIC This step selects the columns needed to choose an optimized option.
# MAGIC
# MAGIC The optimization is based on `total_logistics_cost`.
# MAGIC
# MAGIC Warehouse capacity is included as `warehouse_daily_order_capacity`, which represents the maximum number of orders that a plant can process per day.

# COMMAND ----------

# DBTITLE 1,Prepare valid options for optimization
optimization_options_df = (
    fact_freight_cost_option_df
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
        "warehouse_daily_order_capacity",
        "is_vmi_customer",
        "is_candidate_plant_vmi_restricted",
        "freight_cost",
        "warehouse_handling_cost",
        "total_logistics_cost"
    )
    .filter(F.col("total_logistics_cost").isNotNull())
    .filter(F.col("warehouse_daily_order_capacity").isNotNull())
)

print(f"Optimization option rows: {optimization_options_df.count()}")

display(optimization_options_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Rank valid options by cost per order
# MAGIC
# MAGIC This step ranks valid options from cheapest to most expensive for each order.
# MAGIC
# MAGIC The rank helps identify whether the final optimized assignment selected the lowest-cost option or a more expensive option due to capacity constraints.

# COMMAND ----------

# DBTITLE 1,Rank valid options by cost per order
option_rank_window = (
    Window
    .partitionBy("order_id")
    .orderBy(
        F.col("total_logistics_cost").asc(),
        F.col("candidate_tpt_day_count").asc(),
        F.col("candidate_plant_code").asc(),
        F.col("candidate_carrier").asc()
    )
)

ranked_options_df = (
    optimization_options_df
    .withColumn("optimized_option_rank", F.row_number().over(option_rank_window))
)

print(f"Ranked optimization option rows: {ranked_options_df.count()}")
display(ranked_options_df.orderBy("order_id", "optimized_option_rank").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Apply capacity-aware greedy assignment
# MAGIC
# MAGIC This step assigns one optimized option to each order.
# MAGIC
# MAGIC For each order, the algorithm selects the lowest-cost option that still has at least one available order-capacity slot for the order date and candidate plant.
# MAGIC
# MAGIC Warehouse capacity is measured in number of orders per day, so each assigned order consumes exactly one capacity slot.

# COMMAND ----------

# DBTITLE 1,Apply capacity-aware greedy assignment
ranked_options_pd = (
    ranked_options_df
    .orderBy(
        "order_date",
        "order_id",
        "optimized_option_rank"
    )
    .toPandas()
)

selected_assignments = []
unassigned_orders = []
remaining_order_capacity = {}

for order_id, order_options in ranked_options_pd.groupby("order_id", sort=False):
    selected_option = None

    for _, option in order_options.sort_values("optimized_option_rank").iterrows():
        capacity_key = (
            str(option["order_date"]),
            option["candidate_plant_code"]
        )

        if capacity_key not in remaining_order_capacity:
            remaining_order_capacity[capacity_key] = int(option["warehouse_daily_order_capacity"])

        required_order_capacity = 1

        if remaining_order_capacity[capacity_key] >= required_order_capacity:
            selected_option = option.copy()
            remaining_order_capacity[capacity_key] -= required_order_capacity
            break

    if selected_option is not None:
        selected_assignments.append(selected_option)
    else:
        unassigned_orders.append(order_id)

optimized_assignment_base_pd = pd.DataFrame(selected_assignments)
unassigned_orders_pd = pd.DataFrame({"order_id": unassigned_orders})

if optimized_assignment_base_pd.empty:
    raise ValueError("No optimized assignments were created.")

optimized_assignment_base_df = spark.createDataFrame(optimized_assignment_base_pd)

if not unassigned_orders_pd.empty:
    unassigned_orders_df = spark.createDataFrame(unassigned_orders_pd)
else:
    unassigned_orders_df = spark.createDataFrame([], "order_id string")

optimized_assigned_orders_count = (
    optimized_assignment_base_df
    .select("order_id")
    .dropDuplicates()
    .count()
)

unassigned_orders_count = unassigned_orders_df.count()

print(f"Optimized assigned orders: {optimized_assigned_orders_count}")
print(f"Unassigned orders due to capacity: {unassigned_orders_count}")

display(optimized_assignment_base_df.limit(10))
display(unassigned_orders_df.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Add historical cost values
# MAGIC
# MAGIC This step joins the optimized assignments with the historical cost values from `fact_order_shipment`.
# MAGIC
# MAGIC Historical cost is needed to calculate estimated savings.

# COMMAND ----------

# DBTITLE 1,Add historical cost values
historical_cost_df = (
    fact_order_shipment_df
    .select(
        "order_id",
        F.col("historical_freight_cost"),
        F.col("historical_warehouse_handling_cost"),
        F.col("historical_total_logistics_cost").alias("historical_total_cost")
    )
)

optimized_assignment_with_history_df = (
    optimized_assignment_base_df
    .join(
        historical_cost_df,
        on="order_id",
        how="left"
    )
)

display(optimized_assignment_with_history_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Create the optimized order assignment fact
# MAGIC
# MAGIC This step creates the final optimized assignment table.
# MAGIC
# MAGIC The selected candidate route is renamed with the `optimized_` prefix to make the comparison with the historical route clear.

# COMMAND ----------

# DBTITLE 1,Create the optimized order assignment fact
fact_optimized_order_assignment_df = (
    optimized_assignment_with_history_df
    .withColumn(
        "optimized_total_cost",
        F.col("total_logistics_cost").cast("double")
    )
    .withColumn(
        "estimated_savings",
        F.when(
            F.col("historical_total_cost").isNotNull(),
            F.col("historical_total_cost") - F.col("optimized_total_cost")
        )
    )
    .withColumn(
        "savings_percentage",
        F.when(
            (F.col("historical_total_cost").isNotNull()) &
            (F.col("historical_total_cost") > 0),
            F.round(F.col("estimated_savings") / F.col("historical_total_cost"), 4)
        )
    )
    .withColumn(
        "is_lowest_cost_option",
        F.col("optimized_option_rank") == 1
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
        F.col("candidate_plant_code").alias("optimized_plant_code"),
        F.col("candidate_orig_port").alias("optimized_orig_port"),
        F.col("candidate_carrier").alias("optimized_carrier"),
        F.col("candidate_transport_mode").alias("optimized_transport_mode"),
        F.col("candidate_carrier_type").alias("optimized_carrier_type"),
        F.col("candidate_tpt_day_count").alias("optimized_tpt_day_count"),
        "optimized_option_rank",
        "is_lowest_cost_option",
        "is_vmi_customer",
        "is_candidate_plant_vmi_restricted",
        F.col("warehouse_daily_order_capacity").alias("optimized_warehouse_daily_order_capacity"),
        "historical_freight_cost",
        "historical_warehouse_handling_cost",
        "historical_total_cost",
        F.col("freight_cost").alias("optimized_freight_cost"),
        F.col("warehouse_handling_cost").alias("optimized_warehouse_handling_cost"),
        "optimized_total_cost",
        "estimated_savings",
        "savings_percentage"
    )
)

print(f"fact_optimized_order_assignment rows: {fact_optimized_order_assignment_df.count()}")

display(fact_optimized_order_assignment_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Review optimized assignment coverage
# MAGIC
# MAGIC This step reviews how many historical orders were assigned by the optimization.
# MAGIC
# MAGIC Since warehouse capacity is a hard constraint, some orders may remain unassigned.
# MAGIC
# MAGIC Unassigned orders are not treated as a technical failure in this notebook. They represent demand that could not be covered under the current network and capacity constraints.
# MAGIC
# MAGIC Duplicate optimized assignments are still treated as a critical error.

# COMMAND ----------

# DBTITLE 1,Validate one optimized assignment per order
optimized_assignment_order_count = (
    fact_optimized_order_assignment_df
    .select("order_id")
    .dropDuplicates()
    .count()
)

gold_order_count = (
    fact_order_shipment_df
    .select("order_id")
    .dropDuplicates()
    .count()
)

duplicate_optimized_assignments_df = (
    fact_optimized_order_assignment_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_optimized_assignment_count = duplicate_optimized_assignments_df.count()

missing_optimized_orders_df = (
    fact_order_shipment_df
    .select("order_id")
    .dropDuplicates()
    .join(
        fact_optimized_order_assignment_df.select("order_id").dropDuplicates(),
        on="order_id",
        how="left_anti"
    )
)

missing_optimized_order_count = missing_optimized_orders_df.count()

assignment_coverage_percentage = round(
    optimized_assignment_order_count / gold_order_count,
    4
)

print(f"Gold historical orders:            {gold_order_count}")
print(f"Optimized assigned orders:         {optimized_assignment_order_count}")
print(f"Unassigned orders:                 {missing_optimized_order_count}")
print(f"Assignment coverage percentage:    {assignment_coverage_percentage}")
print(f"Duplicate optimized assignments:   {duplicate_optimized_assignment_count}")

display(duplicate_optimized_assignments_df)
display(missing_optimized_orders_df.limit(50))

if duplicate_optimized_assignment_count > 0:
    raise ValueError("Duplicate optimized assignments found.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Write Optimization tables
# MAGIC
# MAGIC This step writes the optimization outputs as external Delta tables registered in Unity Catalog.
# MAGIC
# MAGIC The logical table names and physical paths come from `00_config`.
# MAGIC
# MAGIC `overwrite` is used because the optimization result is rebuilt completely from the current Gold inputs.

# COMMAND ----------

# DBTITLE 1,Write Optimization tables
optimization_tables_to_write = {
    "fact_optimized_order_assignment": fact_optimized_order_assignment_df
}

for table_name, dataframe in optimization_tables_to_write.items():
    target_table = OPTIMIZATION_TABLES[table_name]
    target_path = OPTIMIZATION_TABLE_PATHS[table_name]

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", target_path)
        .saveAsTable(target_table)
    )

    print(f"Written Optimization table: {target_table}")
    print(f"Physical path: {target_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Validate written Optimization tables
# MAGIC
# MAGIC This step reads each Optimization table back from Unity Catalog and returns the final row counts.
# MAGIC
# MAGIC This confirms that both optimization outputs were written and registered correctly.

# COMMAND ----------

# DBTITLE 1,Validate written Optimization tables
optimization_validation = []

for table_name in optimization_tables_to_write.keys():
    target_table = OPTIMIZATION_TABLES[table_name]
    target_path = OPTIMIZATION_TABLE_PATHS[table_name]
    row_count = spark.table(target_table).count()

    optimization_validation.append({
        "optimization_table": table_name,
        "unity_catalog_table": target_table,
        "physical_path": target_path,
        "row_count": row_count
    })

optimization_validation_df = spark.createDataFrame(optimization_validation)

display(optimization_validation_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 - List tables in the Optimization schema
# MAGIC
# MAGIC This final step lists the tables currently available in `az_supplychain.optimization`.
# MAGIC
# MAGIC The expected result is the optimized order assignment fact and the cost comparison fact.

# COMMAND ----------

# DBTITLE 1,List tables in the Optimization schema
display(spark.sql(f"SHOW TABLES IN {UC_OPTIMIZATION}"))