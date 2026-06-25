# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 08_optimization_diagnostics
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook creates the diagnostic layer for the Az-SupplyChain optimization model.
# MAGIC
# MAGIC It explains why the optimization assigned some orders and left others unassigned under the current network and warehouse order capacity constraints.
# MAGIC
# MAGIC ## Inputs
# MAGIC
# MAGIC Gold tables:
# MAGIC
# MAGIC - `az_supplychain.gold.fact_order_shipment`
# MAGIC - `az_supplychain.gold.fact_freight_cost_option`
# MAGIC - `az_supplychain.gold.dim_plant`
# MAGIC
# MAGIC Optimization table from notebook `07_optimization_model`:
# MAGIC
# MAGIC - `az_supplychain.optimization.fact_optimized_order_assignment`
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC Optimization diagnostic tables:
# MAGIC
# MAGIC - `az_supplychain.optimization.fact_optimized_capacity_usage`
# MAGIC - `az_supplychain.optimization.fact_unassigned_order_diagnostic`
# MAGIC - `az_supplychain.optimization.fact_cost_comparison`
# MAGIC
# MAGIC ## Processing flow
# MAGIC
# MAGIC 1. Load shared project configuration.
# MAGIC 2. Read Gold and Optimization tables.
# MAGIC 3. Rebuild option ranking from Gold freight cost options.
# MAGIC 4. Identify assigned and unassigned orders.
# MAGIC 5. Create optimized plant capacity usage diagnostics.
# MAGIC 6. Create unassigned order diagnostics.
# MAGIC 7. Analyze selected option rank.
# MAGIC 8. Analyze unassigned demand by product, service level and destination.
# MAGIC 9. Create the cost comparison summary.
# MAGIC 10. Validate and write diagnostic outputs.
# MAGIC
# MAGIC ## Important notes
# MAGIC
# MAGIC - This notebook does not change optimized assignments.
# MAGIC - It only explains the optimization result.
# MAGIC - Warehouse capacity is measured as number of orders per day.
# MAGIC - Unassigned orders are expected when capacity is a hard constraint.
# MAGIC - This model is a capacity-aware greedy heuristic, not a global LP solver.

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
# MAGIC ## Step 2 - Import PySpark functions
# MAGIC
# MAGIC This step imports the PySpark functions needed for diagnostic calculations.

# COMMAND ----------

# DBTITLE 1,Import PySpark functions
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Read Gold and Optimization tables
# MAGIC
# MAGIC This step reads the tables needed to explain the optimization result.
# MAGIC
# MAGIC The optimized assignment table is created by notebook `07_optimization_model`.

# COMMAND ----------

# DBTITLE 1,Read Gold and Optimization tables
fact_order_shipment_df = spark.table(GOLD_TABLES["fact_order_shipment"])
fact_freight_cost_option_df = spark.table(GOLD_TABLES["fact_freight_cost_option"])
dim_plant_df = spark.table(GOLD_TABLES["dim_plant"])

fact_optimized_order_assignment_df = spark.table(
    OPTIMIZATION_TABLES["fact_optimized_order_assignment"]
)

print(f"Gold fact_order_shipment rows:              {fact_order_shipment_df.count()}")
print(f"Gold fact_freight_cost_option rows:         {fact_freight_cost_option_df.count()}")
print(f"Gold dim_plant rows:                        {dim_plant_df.count()}")
print(f"Optimized assignment rows:                  {fact_optimized_order_assignment_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Rebuild valid option ranking
# MAGIC
# MAGIC This step rebuilds the option ranking from Gold freight cost options.
# MAGIC
# MAGIC The ranking is needed for diagnostics, but it does not change the optimized assignment already created in notebook 07.

# COMMAND ----------

# DBTITLE 1,Rebuild valid option ranking
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
    fact_freight_cost_option_df
    .withColumn("optimized_option_rank", F.row_number().over(option_rank_window))
)

print(f"Ranked option rows: {ranked_options_df.count()}")

display(ranked_options_df.orderBy("order_id", "optimized_option_rank").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Identify assigned and unassigned orders
# MAGIC
# MAGIC This step compares historical orders against optimized assignments.
# MAGIC
# MAGIC Unassigned orders represent demand that could not be covered under the current constraints.

# COMMAND ----------

# DBTITLE 1,Identify assigned and unassigned orders
historical_orders_df = (
    fact_order_shipment_df
    .select("order_id")
    .dropDuplicates()
)

optimized_orders_df = (
    fact_optimized_order_assignment_df
    .select("order_id")
    .dropDuplicates()
)

missing_optimized_orders_df = (
    historical_orders_df
    .join(
        optimized_orders_df,
        on="order_id",
        how="left_anti"
    )
)

historical_order_count = historical_orders_df.count()
optimized_order_count = optimized_orders_df.count()
missing_optimized_order_count = missing_optimized_orders_df.count()

assignment_coverage_percentage = round(
    optimized_order_count / historical_order_count,
    4
)

print(f"Historical orders:             {historical_order_count}")
print(f"Optimized assigned orders:     {optimized_order_count}")
print(f"Unassigned orders:             {missing_optimized_order_count}")
print(f"Assignment coverage percentage:{assignment_coverage_percentage}")

display(missing_optimized_orders_df.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Create optimized capacity usage diagnostic
# MAGIC
# MAGIC This step creates a plant-date diagnostic using the valid logistics options already generated before optimization.
# MAGIC
# MAGIC It explains:
# MAGIC
# MAGIC - how many orders had each plant as a valid candidate
# MAGIC - how many orders were optimized into each plant
# MAGIC - how many unassigned orders had each plant as a candidate
# MAGIC - how much order capacity remains available
# MAGIC - whether the plant is a bottleneck or underused

# COMMAND ----------

# DBTITLE 1,Create optimized capacity usage diagnostic
plant_date_capacity_df = (
    fact_order_shipment_df
    .select("date_key", "order_date")
    .dropDuplicates()
    .crossJoin(
        dim_plant_df
        .select(
            "plant_code",
            "daily_order_capacity"
        )
        .filter(F.col("daily_order_capacity").isNotNull())
    )
)

candidate_order_by_plant_df = (
    ranked_options_df
    .select(
        "date_key",
        "order_date",
        F.col("candidate_plant_code").alias("plant_code"),
        "order_id"
    )
    .dropDuplicates()
    .groupBy(
        "date_key",
        "order_date",
        "plant_code"
    )
    .agg(
        F.countDistinct("order_id").alias("candidate_order_count")
    )
)

assigned_order_by_plant_df = (
    fact_optimized_order_assignment_df
    .select(
        "date_key",
        "order_date",
        F.col("optimized_plant_code").alias("plant_code"),
        "order_id",
        "unit_quant",
        "weight"
    )
    .groupBy(
        "date_key",
        "order_date",
        "plant_code"
    )
    .agg(
        F.countDistinct("order_id").alias("optimized_order_count"),
        F.sum("unit_quant").alias("optimized_total_unit_quantity"),
        F.sum("weight").alias("optimized_total_weight")
    )
)

unassigned_candidate_by_plant_df = (
    missing_optimized_orders_df
    .join(
        ranked_options_df
        .select(
            "order_id",
            "date_key",
            "order_date",
            F.col("candidate_plant_code").alias("plant_code")
        )
        .dropDuplicates(),
        on="order_id",
        how="inner"
    )
    .groupBy(
        "date_key",
        "order_date",
        "plant_code"
    )
    .agg(
        F.countDistinct("order_id").alias("unassigned_candidate_order_count")
    )
)

fact_optimized_capacity_usage_df = (
    plant_date_capacity_df
    .join(
        candidate_order_by_plant_df,
        on=["date_key", "order_date", "plant_code"],
        how="left"
    )
    .join(
        assigned_order_by_plant_df,
        on=["date_key", "order_date", "plant_code"],
        how="left"
    )
    .join(
        unassigned_candidate_by_plant_df,
        on=["date_key", "order_date", "plant_code"],
        how="left"
    )
    .fillna({
        "candidate_order_count": 0,
        "optimized_order_count": 0,
        "optimized_total_unit_quantity": 0,
        "optimized_total_weight": 0,
        "unassigned_candidate_order_count": 0
    })
    .withColumn(
        "assigned_elsewhere_candidate_order_count",
        F.greatest(
            F.lit(0),
            F.col("candidate_order_count")
            - F.col("optimized_order_count")
            - F.col("unassigned_candidate_order_count")
        )
    )
    .withColumn(
        "optimized_capacity_usage_ratio",
        F.round(
            F.col("optimized_order_count") / F.col("daily_order_capacity"),
            4
        )
    )
    .withColumn(
        "candidate_capacity_pressure_ratio",
        F.round(
            F.col("candidate_order_count") / F.col("daily_order_capacity"),
            4
        )
    )
    .withColumn(
        "remaining_order_capacity",
        F.col("daily_order_capacity") - F.col("optimized_order_count")
    )
    .withColumn(
        "is_capacity_exceeded",
        F.col("optimized_order_count") > F.col("daily_order_capacity")
    )
    .withColumn(
        "capacity_diagnostic_status",
        F.when(
            F.col("is_capacity_exceeded") == True,
            F.lit("CAPACITY_EXCEEDED")
        )
        .when(
            F.col("candidate_order_count") == 0,
            F.lit("NO_CANDIDATE_DEMAND")
        )
        .when(
            (F.col("remaining_order_capacity") == 0) &
            (F.col("unassigned_candidate_order_count") > 0),
            F.lit("CAPACITY_BOTTLENECK")
        )
        .when(
            F.col("remaining_order_capacity") == 0,
            F.lit("FULLY_USED")
        )
        .when(
            (F.col("remaining_order_capacity") > 0) &
            (F.col("candidate_order_count") <= F.col("daily_order_capacity")),
            F.lit("UNDER_USED_CANDIDATE_DEMAND_BELOW_CAPACITY")
        )
        .when(
            (F.col("remaining_order_capacity") > 0) &
            (F.col("unassigned_candidate_order_count") == 0) &
            (F.col("assigned_elsewhere_candidate_order_count") > 0),
            F.lit("UNDER_USED_NOT_COST_PREFERRED")
        )
        .when(
            (F.col("remaining_order_capacity") > 0) &
            (F.col("unassigned_candidate_order_count") > 0),
            F.lit("REVIEW_UNUSED_CAPACITY_WITH_UNASSIGNED_CANDIDATES")
        )
        .otherwise(F.lit("REVIEW_REQUIRED"))
    )
    .select(
        "date_key",
        "order_date",
        "plant_code",
        "daily_order_capacity",
        "candidate_order_count",
        "optimized_order_count",
        "unassigned_candidate_order_count",
        "assigned_elsewhere_candidate_order_count",
        "optimized_total_unit_quantity",
        "optimized_total_weight",
        "optimized_capacity_usage_ratio",
        "candidate_capacity_pressure_ratio",
        "remaining_order_capacity",
        "is_capacity_exceeded",
        "capacity_diagnostic_status"
    )
)

capacity_exceeded_count = (
    fact_optimized_capacity_usage_df
    .filter(F.col("is_capacity_exceeded") == True)
    .count()
)

print(f"Optimized plant-date capacity rows exceeded: {capacity_exceeded_count}")

display(
    fact_optimized_capacity_usage_df
    .orderBy(F.col("candidate_capacity_pressure_ratio").desc(), "plant_code")
)

if capacity_exceeded_count > 0:
    raise ValueError("Optimized assignments exceed warehouse daily order capacity.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Review plant concentration and unused capacity
# MAGIC
# MAGIC This step reviews critical plants and plants with unused capacity.
# MAGIC
# MAGIC It helps explain whether unused capacity is caused by low candidate demand, cost ranking, or true capacity bottlenecks.

# COMMAND ----------

# DBTITLE 1,Review plant concentration and unused capacity
display(
    fact_optimized_capacity_usage_df
    .filter(F.col("plant_code").isin("PLANT03", "PLANT18"))
)

display(
    fact_optimized_capacity_usage_df
    .orderBy(F.col("candidate_capacity_pressure_ratio").desc())
)

display(
    fact_optimized_capacity_usage_df
    .filter(F.col("remaining_order_capacity") > 0)
    .orderBy(
        F.col("remaining_order_capacity").desc(),
        F.col("candidate_order_count").asc()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Create unassigned order diagnostic
# MAGIC
# MAGIC This step creates a diagnostic table for orders that were not assigned by the optimization.
# MAGIC
# MAGIC The diagnostic uses valid options already available in `ranked_options_df`.
# MAGIC
# MAGIC Main diagnostic reasons:
# MAGIC
# MAGIC - `CAPACITY_BLOCKED`
# MAGIC - `NO_VALID_OPTIONS_AFTER_NETWORK_CONSTRAINTS`
# MAGIC - `POTENTIAL_GREEDY_ISSUE`
# MAGIC - `REVIEW_REQUIRED`

# COMMAND ----------

# DBTITLE 1,Create unassigned order diagnostic
unassigned_order_base_df = (
    fact_order_shipment_df
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
        F.col("plant_code").alias("historical_plant_code"),
        F.col("orig_port").alias("historical_orig_port"),
        F.col("carrier").alias("historical_carrier")
    )
    .join(
        fact_optimized_order_assignment_df
        .select("order_id")
        .dropDuplicates(),
        on="order_id",
        how="left_anti"
    )
)

option_summary_by_order_df = (
    ranked_options_df
    .groupBy("order_id")
    .agg(
        F.count("*").alias("valid_option_count"),
        F.countDistinct("candidate_plant_code").alias("candidate_plant_count"),
        F.array_join(
            F.sort_array(F.collect_set("candidate_plant_code")),
            ", "
        ).alias("candidate_plants"),
        F.min("total_logistics_cost").alias("minimum_available_option_cost")
    )
)

candidate_capacity_status_by_order_df = (
    unassigned_order_base_df
    .select("order_id")
    .join(
        ranked_options_df
        .select(
            "order_id",
            "order_date",
            F.col("candidate_plant_code").alias("plant_code")
        )
        .dropDuplicates(),
        on="order_id",
        how="left"
    )
    .join(
        fact_optimized_capacity_usage_df
        .select(
            "order_date",
            "plant_code",
            "remaining_order_capacity"
        ),
        on=["order_date", "plant_code"],
        how="left"
    )
    .groupBy("order_id")
    .agg(
        F.sum(
            F.when(F.col("remaining_order_capacity") <= 0, 1).otherwise(0)
        ).alias("full_candidate_plant_count"),
        F.sum(
            F.when(F.col("remaining_order_capacity") > 0, 1).otherwise(0)
        ).alias("available_candidate_plant_count")
    )
)

fact_unassigned_order_diagnostic_df = (
    unassigned_order_base_df
    .join(
        option_summary_by_order_df,
        on="order_id",
        how="left"
    )
    .join(
        candidate_capacity_status_by_order_df,
        on="order_id",
        how="left"
    )
    .fillna({
        "valid_option_count": 0,
        "candidate_plant_count": 0,
        "candidate_plants": "NO_CANDIDATE_PLANTS",
        "full_candidate_plant_count": 0,
        "available_candidate_plant_count": 0
    })
    .withColumn(
        "constraint_diagnostic_reason",
        F.when(
            F.col("valid_option_count") == 0,
            F.lit("NO_VALID_OPTIONS_AFTER_NETWORK_CONSTRAINTS")
        )
        .when(
            F.col("available_candidate_plant_count") > 0,
            F.lit("POTENTIAL_GREEDY_ISSUE")
        )
        .when(
            F.col("full_candidate_plant_count") == F.col("candidate_plant_count"),
            F.lit("CAPACITY_BLOCKED")
        )
        .otherwise(F.lit("REVIEW_REQUIRED"))
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
        "valid_option_count",
        "candidate_plant_count",
        "candidate_plants",
        "full_candidate_plant_count",
        "available_candidate_plant_count",
        "minimum_available_option_cost",
        "constraint_diagnostic_reason"
    )
)

print(f"Unassigned order diagnostic rows: {fact_unassigned_order_diagnostic_df.count()}")

display(
    fact_unassigned_order_diagnostic_df
    .groupBy("constraint_diagnostic_reason")
    .count()
    .orderBy(F.col("count").desc())
)

display(fact_unassigned_order_diagnostic_df.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Analyze selected option rank
# MAGIC
# MAGIC This step analyzes which option ranks were selected by the greedy optimizer.
# MAGIC
# MAGIC If ranks greater than 1 appear, it means the optimizer used alternative options when cheaper options were blocked by capacity.

# COMMAND ----------

# DBTITLE 1,Analyze selected option rank
selected_rank_distribution_df = (
    fact_optimized_order_assignment_df
    .groupBy("optimized_option_rank")
    .agg(
        F.countDistinct("order_id").alias("optimized_order_count")
    )
    .withColumn(
        "optimized_order_percentage",
        F.round(
            F.col("optimized_order_count") /
            fact_optimized_order_assignment_df.select("order_id").dropDuplicates().count(),
            4
        )
    )
    .orderBy("optimized_option_rank")
)

display(selected_rank_distribution_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Analyze unassigned demand by product, service level and destination
# MAGIC
# MAGIC This step summarizes unassigned demand by key business attributes.
# MAGIC
# MAGIC It helps identify whether unassigned orders are concentrated around specific products, service levels or destination ports.

# COMMAND ----------

# DBTITLE 1,Analyze unassigned demand by product, service level and destination
display(
    fact_unassigned_order_diagnostic_df
    .groupBy("constraint_diagnostic_reason")
    .agg(
        F.countDistinct("order_id").alias("unassigned_order_count"),
        F.sum("unit_quant").alias("unassigned_total_unit_quantity"),
        F.sum("weight").alias("unassigned_total_weight")
    )
    .orderBy(F.col("unassigned_order_count").desc())
)

display(
    fact_unassigned_order_diagnostic_df
    .groupBy(
        "constraint_diagnostic_reason",
        "service_level"
    )
    .agg(
        F.countDistinct("order_id").alias("unassigned_order_count"),
        F.sum("unit_quant").alias("unassigned_total_unit_quantity"),
        F.sum("weight").alias("unassigned_total_weight")
    )
    .orderBy(F.col("unassigned_order_count").desc())
)

display(
    fact_unassigned_order_diagnostic_df
    .groupBy(
        "constraint_diagnostic_reason",
        "dest_port"
    )
    .agg(
        F.countDistinct("order_id").alias("unassigned_order_count"),
        F.sum("unit_quant").alias("unassigned_total_unit_quantity"),
        F.sum("weight").alias("unassigned_total_weight")
    )
    .orderBy(F.col("unassigned_order_count").desc())
)

display(
    fact_unassigned_order_diagnostic_df
    .groupBy(
        "constraint_diagnostic_reason",
        "product_id"
    )
    .agg(
        F.countDistinct("order_id").alias("unassigned_order_count"),
        F.sum("unit_quant").alias("unassigned_total_unit_quantity"),
        F.sum("weight").alias("unassigned_total_weight")
    )
    .orderBy(F.col("unassigned_order_count").desc())
    .limit(50)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Review PORT09 unassigned demand
# MAGIC
# MAGIC This step reviews unassigned demand for destination port `PORT09`.
# MAGIC
# MAGIC The purpose is to confirm whether PORT09 is the main destination affected by network and capacity constraints.

# COMMAND ----------

# DBTITLE 1,Review PORT09 unassigned demand
port09_unassigned_detail_df = (
    fact_unassigned_order_diagnostic_df
    .filter(F.col("dest_port") == "PORT09")
    .groupBy(
        "constraint_diagnostic_reason",
        "candidate_plant_count",
        "candidate_plants"
    )
    .agg(
        F.countDistinct("order_id").alias("unassigned_order_count"),
        F.sum("unit_quant").alias("unassigned_total_unit_quantity"),
        F.sum("weight").alias("unassigned_total_weight")
    )
    .orderBy(F.col("unassigned_order_count").desc())
)

display(port09_unassigned_detail_df.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 - Create the cost comparison fact
# MAGIC
# MAGIC This step creates a summary comparison between historical cost and optimized cost.
# MAGIC
# MAGIC Only assigned orders with both historical and optimized cost are included in cost totals.
# MAGIC
# MAGIC The summary also includes assignment coverage metrics to explain how much demand was covered under current capacity constraints.

# COMMAND ----------

# DBTITLE 1,Create the cost comparison fact
orders_with_historical_cost_df = (
    fact_optimized_order_assignment_df
    .filter(F.col("historical_total_cost").isNotNull())
)

orders_without_historical_cost_count = (
    fact_optimized_order_assignment_df
    .filter(F.col("historical_total_cost").isNull())
    .count()
)

orders_with_valid_options_count = (
    ranked_options_df
    .select("order_id")
    .dropDuplicates()
    .count()
)

fact_cost_comparison_df = (
    orders_with_historical_cost_df
    .agg(
        F.count("order_id").alias("orders_compared"),
        F.sum("historical_total_cost").alias("historical_total_cost"),
        F.sum("optimized_total_cost").alias("optimized_total_cost")
    )
    .withColumn(
        "comparison_scope",
        F.lit("assigned_orders_with_historical_and_optimized_cost")
    )
    .withColumn(
        "historical_order_count",
        F.lit(historical_order_count)
    )
    .withColumn(
        "orders_with_valid_options",
        F.lit(orders_with_valid_options_count)
    )
    .withColumn(
        "orders_with_optimized_assignment",
        F.lit(optimized_order_count)
    )
    .withColumn(
        "orders_unassigned_under_constraints",
        F.lit(missing_optimized_order_count)
    )
    .withColumn(
        "orders_without_historical_cost",
        F.lit(orders_without_historical_cost_count)
    )
    .withColumn(
        "assignment_coverage_percentage",
        F.round(
            F.col("orders_with_optimized_assignment") / F.col("historical_order_count"),
            4
        )
    )
    .withColumn(
        "estimated_savings",
        F.col("historical_total_cost") - F.col("optimized_total_cost")
    )
    .withColumn(
        "savings_percentage",
        F.when(
            F.col("historical_total_cost") > 0,
            F.round(F.col("estimated_savings") / F.col("historical_total_cost"), 4)
        )
    )
    .select(
        "comparison_scope",
        "historical_order_count",
        "orders_with_valid_options",
        "orders_with_optimized_assignment",
        "orders_unassigned_under_constraints",
        "orders_compared",
        "orders_without_historical_cost",
        "assignment_coverage_percentage",
        "historical_total_cost",
        "optimized_total_cost",
        "estimated_savings",
        "savings_percentage"
    )
)

display(fact_cost_comparison_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 - Create final optimization diagnostic summary
# MAGIC
# MAGIC This step creates a compact summary of the optimization result.
# MAGIC
# MAGIC It is displayed in the notebook and helps interpret the final optimization coverage.

# COMMAND ----------

# DBTITLE 1,Create final optimization diagnostic summary
capacity_blocked_order_count = (
    fact_unassigned_order_diagnostic_df
    .filter(F.col("constraint_diagnostic_reason") == "CAPACITY_BLOCKED")
    .select("order_id")
    .dropDuplicates()
    .count()
)

no_valid_option_order_count = (
    fact_unassigned_order_diagnostic_df
    .filter(F.col("constraint_diagnostic_reason") == "NO_VALID_OPTIONS_AFTER_NETWORK_CONSTRAINTS")
    .select("order_id")
    .dropDuplicates()
    .count()
)

potential_greedy_issue_order_count = (
    fact_unassigned_order_diagnostic_df
    .filter(F.col("constraint_diagnostic_reason") == "POTENTIAL_GREEDY_ISSUE")
    .select("order_id")
    .dropDuplicates()
    .count()
)

optimization_diagnostic_summary = [
    {
        "metric_name": "historical_order_count",
        "metric_value": float(historical_order_count)
    },
    {
        "metric_name": "optimized_order_count",
        "metric_value": float(optimized_order_count)
    },
    {
        "metric_name": "unassigned_order_count",
        "metric_value": float(missing_optimized_order_count)
    },
    {
        "metric_name": "capacity_blocked_order_count",
        "metric_value": float(capacity_blocked_order_count)
    },
    {
        "metric_name": "no_valid_option_order_count",
        "metric_value": float(no_valid_option_order_count)
    },
    {
        "metric_name": "potential_greedy_issue_order_count",
        "metric_value": float(potential_greedy_issue_order_count)
    },
    {
        "metric_name": "assignment_coverage_percentage",
        "metric_value": float(assignment_coverage_percentage)
    }
]

optimization_diagnostic_summary_df = spark.createDataFrame(optimization_diagnostic_summary)

display(optimization_diagnostic_summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14 - Validate diagnostic outputs
# MAGIC
# MAGIC This step validates that diagnostic outputs are consistent.
# MAGIC
# MAGIC Capacity exceeded is a critical error.
# MAGIC
# MAGIC Potential greedy issues are not treated as a failure here, but they should be reviewed if present.

# COMMAND ----------

# DBTITLE 1,Validate diagnostic outputs
potential_greedy_issue_count = potential_greedy_issue_order_count

print(f"Capacity exceeded rows:        {capacity_exceeded_count}")
print(f"Potential greedy issue orders: {potential_greedy_issue_count}")

if capacity_exceeded_count > 0:
    raise ValueError("Capacity exceeded in optimized capacity usage diagnostic.")

display(
    fact_unassigned_order_diagnostic_df
    .filter(F.col("constraint_diagnostic_reason") == "POTENTIAL_GREEDY_ISSUE")
    .limit(50)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15 - Write Optimization diagnostic tables
# MAGIC
# MAGIC This step writes the Optimization diagnostic outputs as external Delta tables registered in Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Write Optimization diagnostic tables
optimization_diagnostic_tables_to_write = {
    "fact_optimized_capacity_usage": fact_optimized_capacity_usage_df,
    "fact_unassigned_order_diagnostic": fact_unassigned_order_diagnostic_df,
    "fact_cost_comparison": fact_cost_comparison_df
}

for table_name, dataframe in optimization_diagnostic_tables_to_write.items():
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

    print(f"Written Optimization diagnostic table: {target_table}")
    print(f"Physical path: {target_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 16 - Validate written Optimization diagnostic tables
# MAGIC
# MAGIC This step reads the diagnostic tables back from Unity Catalog and confirms their row counts.

# COMMAND ----------

# DBTITLE 1,Validate written Optimization diagnostic tables
optimization_diagnostic_validation = []

for table_name in optimization_diagnostic_tables_to_write.keys():
    target_table = OPTIMIZATION_TABLES[table_name]
    target_path = OPTIMIZATION_TABLE_PATHS[table_name]
    row_count = spark.table(target_table).count()

    optimization_diagnostic_validation.append({
        "optimization_table": table_name,
        "unity_catalog_table": target_table,
        "physical_path": target_path,
        "row_count": row_count
    })

optimization_diagnostic_validation_df = spark.createDataFrame(
    optimization_diagnostic_validation
)

display(optimization_diagnostic_validation_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 17 - List tables in the Optimization schema
# MAGIC
# MAGIC This final step lists the tables currently available in the Optimization schema.

# COMMAND ----------

# DBTITLE 1,List tables in the Optimization schema
display(spark.sql(f"SHOW TABLES IN {UC_OPTIMIZATION}"))