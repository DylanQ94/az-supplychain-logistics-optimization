# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 04_create_valid_shipping_options
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook creates the Silver `valid_shipping_options` table.
# MAGIC
# MAGIC The table represents all valid logistics options for each order based on product-plant compatibility, plant-port connectivity, freight rate applicability, warehouse costs, warehouse capacity availability and VMI customer rules.
# MAGIC
# MAGIC ## Inputs
# MAGIC
# MAGIC Silver tables:
# MAGIC
# MAGIC - `az_supplychain.silver.order_list`
# MAGIC - `az_supplychain.silver.products_per_plant`
# MAGIC - `az_supplychain.silver.plant_ports`
# MAGIC - `az_supplychain.silver.freight_rates`
# MAGIC - `az_supplychain.silver.wh_costs`
# MAGIC - `az_supplychain.silver.wh_capacities`
# MAGIC - `az_supplychain.silver.vmi_customers`
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC - `az_supplychain.silver.valid_shipping_options`
# MAGIC
# MAGIC ## Processing flow
# MAGIC
# MAGIC 1. Load shared project configuration from `00_config`.
# MAGIC 2. Read the required Silver tables.
# MAGIC 3. Prepare the orders base with historical route information.
# MAGIC 4. Generate candidate plants using product-plant compatibility.
# MAGIC 5. Generate candidate origin ports using plant-port connectivity.
# MAGIC 6. Match applicable freight rates by origin port, destination port, service level and weight range.
# MAGIC 7. Add warehouse costs and warehouse capacity data.
# MAGIC 8. Apply VMI customer rules.
# MAGIC 9. Calculate freight, warehouse and total logistics cost.
# MAGIC 10. Validate that each order has at least one valid shipping option.
# MAGIC 11. Write the final table as an external Delta table registered in Unity Catalog.
# MAGIC 12. Validate the written Silver table.
# MAGIC
# MAGIC ## Important notes
# MAGIC
# MAGIC - This notebook does not choose the optimized option yet.
# MAGIC - This notebook does not create Gold tables.
# MAGIC - The optimization layer will later select the lowest-cost valid option per order.
# MAGIC - The same service level from the original order is preserved.
# MAGIC - No unnecessary technical metadata is carried into the derived table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load project configuration
# MAGIC
# MAGIC This step imports the shared configuration from `00_config`.
# MAGIC
# MAGIC The configuration contains the Unity Catalog table names and the physical ADLS paths where the Silver Delta tables are stored.

# COMMAND ----------

# DBTITLE 1,Load project configuration
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Import PySpark functions
# MAGIC
# MAGIC This step imports the PySpark functions used for joins, calculations and validations.
# MAGIC
# MAGIC The main functions used are:
# MAGIC
# MAGIC - `col()` to reference columns clearly.
# MAGIC - `lit()` to create boolean flags.
# MAGIC - `coalesce()` to handle missing VMI flags.
# MAGIC - `greatest()` to apply the minimum freight cost rule.

# COMMAND ----------

# DBTITLE 1,Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Read required Silver tables
# MAGIC
# MAGIC This step reads the Silver tables needed to build valid shipping options.
# MAGIC
# MAGIC `spark.table()` is used because the tables are already registered in Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Read required Silver tables
silver_orders_df = spark.table(SILVER_TABLES["order_list"])
silver_products_per_plant_df = spark.table(SILVER_TABLES["products_per_plant"])
silver_plant_ports_df = spark.table(SILVER_TABLES["plant_ports"])
silver_freight_rates_df = spark.table(SILVER_TABLES["freight_rates"])
silver_wh_costs_df = spark.table(SILVER_TABLES["wh_costs"])
silver_wh_capacities_df = spark.table(SILVER_TABLES["wh_capacities"])
silver_vmi_customers_df = spark.table(SILVER_TABLES["vmi_customers"])

print(f"Silver orders rows:              {silver_orders_df.count()}")
print(f"Silver products_per_plant rows:  {silver_products_per_plant_df.count()}")
print(f"Silver plant_ports rows:         {silver_plant_ports_df.count()}")
print(f"Silver freight_rates rows:       {silver_freight_rates_df.count()}")
print(f"Silver wh_costs rows:            {silver_wh_costs_df.count()}")
print(f"Silver wh_capacities rows:       {silver_wh_capacities_df.count()}")
print(f"Silver vmi_customers rows:       {silver_vmi_customers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Prepare the orders base
# MAGIC
# MAGIC This step prepares the order columns needed to generate valid shipping options.
# MAGIC
# MAGIC The original `plant_code`, `orig_port`, `carrier` and `tpt_day_count` are renamed with the `historical_` prefix because they describe the historical shipment route.
# MAGIC
# MAGIC Candidate routes will be generated later using constraint tables.

# COMMAND ----------

# DBTITLE 1,Prepare the orders base
orders_base_df = (
    silver_orders_df
    .select(
        "order_id",
        "order_date",
        "customer",
        "product_id",
        "dest_port",
        "unit_quant",
        "weight",
        F.col("service_level"),
        F.col("plant_code").alias("historical_plant_code"),
        F.col("orig_port").alias("historical_orig_port"),
        F.col("carrier").alias("historical_carrier"),
        F.col("tpt_day_count").alias("historical_tpt_day_count"),
        "ship_ahead_day_count",
        "ship_late_day_count"
    )
)

print(f"Orders base rows: {orders_base_df.count()}")
display(orders_base_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Generate candidate plants by product compatibility
# MAGIC
# MAGIC This step joins orders with `products_per_plant`.
# MAGIC
# MAGIC A plant becomes a candidate only if it can handle the product requested by the order.

# COMMAND ----------

# DBTITLE 1,Generate candidate plants by product compatibility
product_plant_options_df = (
    silver_products_per_plant_df
    .select(
        "product_id",
        F.col("plant_code").alias("candidate_plant_code")
    )
    .dropDuplicates(["product_id", "candidate_plant_code"])
)

candidate_plants_df = (
    orders_base_df
    .join(
        product_plant_options_df,
        on="product_id",
        how="inner"
    )
)

print(f"Candidate plant options rows: {candidate_plants_df.count()}")
display(candidate_plants_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Generate candidate origin ports by plant-port connectivity
# MAGIC
# MAGIC This step joins candidate plants with `plant_ports`.
# MAGIC
# MAGIC A candidate plant can only use the ports defined in the plant-port relationship table.

# COMMAND ----------

# DBTITLE 1,Generate candidate origin ports by plant-port connectivity
plant_port_options_df = (
    silver_plant_ports_df
    .select(
        F.col("plant_code").alias("candidate_plant_code"),
        F.col("port").alias("candidate_orig_port")
    )
    .dropDuplicates(["candidate_plant_code", "candidate_orig_port"])
)

candidate_plant_ports_df = (
    candidate_plants_df
    .join(
        plant_port_options_df,
        on="candidate_plant_code",
        how="inner"
    )
)

print(f"Candidate plant-port options rows: {candidate_plant_ports_df.count()}")
display(candidate_plant_ports_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Match applicable freight rates
# MAGIC
# MAGIC This step matches candidate plant-port options with freight rates.
# MAGIC
# MAGIC A freight rate is applicable only when:
# MAGIC
# MAGIC - The candidate origin port matches the freight origin port.
# MAGIC - The order destination port matches the freight destination port.
# MAGIC - The order service level matches the freight service level.
# MAGIC - The order weight falls inside the freight weight range.
# MAGIC
# MAGIC This is the main step that converts network possibilities into valid shipping options.

# COMMAND ----------

# DBTITLE 1,Match applicable freight rates
freight_rates_options_df = (
    silver_freight_rates_df
    .select(
        F.col("carrier").alias("candidate_carrier"),
        F.col("orig_port").alias("freight_orig_port"),
        F.col("dest_port").alias("freight_dest_port"),
        F.col("service_level").alias("freight_service_level"),
        F.col("mode_dsc").alias("candidate_transport_mode"),
        F.col("carrier_type").alias("candidate_carrier_type"),
        F.col("tpt_day_count").alias("candidate_tpt_day_count"),
        F.col("min_weight_quant").alias("freight_min_weight_quant"),
        F.col("max_weight_quant").alias("freight_max_weight_quant"),
        F.col("min_cost").alias("freight_min_cost"),
        F.col("rate").alias("freight_rate")
    )
)

shipping_options_with_rates_df = (
    candidate_plant_ports_df.alias("opt")
    .join(
        freight_rates_options_df.alias("fr"),
        (
            (F.col("opt.candidate_orig_port") == F.col("fr.freight_orig_port")) &
            (F.col("opt.dest_port") == F.col("fr.freight_dest_port")) &
            (F.col("opt.service_level") == F.col("fr.freight_service_level")) &
            (F.col("opt.weight") >= F.col("fr.freight_min_weight_quant")) &
            (F.col("opt.weight") <= F.col("fr.freight_max_weight_quant"))
        ),
        how="inner"
    )
    .select(
        "opt.*",
        "fr.candidate_carrier",
        "fr.candidate_transport_mode",
        "fr.candidate_carrier_type",
        "fr.candidate_tpt_day_count",
        "fr.freight_min_weight_quant",
        "fr.freight_max_weight_quant",
        "fr.freight_min_cost",
        "fr.freight_rate"
    )
)

print(f"Shipping options with applicable rates rows: {shipping_options_with_rates_df.count()}")
display(shipping_options_with_rates_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Add warehouse costs and order capacities
# MAGIC
# MAGIC This step adds warehouse cost and order capacity data to each valid shipping option.
# MAGIC
# MAGIC Warehouse cost is measured in dollars per unit and is used to calculate warehouse handling cost.
# MAGIC
# MAGIC Warehouse capacity is measured in number of orders per day, not units and not weight.
# MAGIC
# MAGIC At this stage, capacity is only attached to each option. The actual capacity usage is evaluated later using order counts.

# COMMAND ----------

# DBTITLE 1,Add warehouse costs and capacities
warehouse_cost_options_df = (
    silver_wh_costs_df
    .select(
        F.col("plant_code").alias("candidate_plant_code"),
        F.col("cost_per_unit").alias("warehouse_cost_per_unit")
    )
)

warehouse_capacity_options_df = (
    silver_wh_capacities_df
    .select(
        F.col("plant_code").alias("candidate_plant_code"),
        F.col("daily_order_capacity").alias("warehouse_daily_order_capacity")
    )
)

shipping_options_with_warehouse_df = (
    shipping_options_with_rates_df
    .join(
        warehouse_cost_options_df,
        on="candidate_plant_code",
        how="inner"
    )
    .join(
        warehouse_capacity_options_df,
        on="candidate_plant_code",
        how="inner"
    )
)

print(f"Shipping options with warehouse data rows: {shipping_options_with_warehouse_df.count()}")

display(shipping_options_with_warehouse_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Apply VMI plant-customer rules
# MAGIC
# MAGIC This step applies the VMI restrictions from the dataset.
# MAGIC
# MAGIC According to the problem definition, VMI rules indicate special cases where a warehouse is only allowed to support specific customers.
# MAGIC
# MAGIC The rule is applied as follows:
# MAGIC
# MAGIC - If a candidate plant appears in `vmi_customers`, the option is valid only when the candidate plant and customer pair exists in the VMI table.
# MAGIC - If a candidate plant does not appear in `vmi_customers`, the plant can support any customer.
# MAGIC
# MAGIC This keeps the logistics options aligned with the current network constraints.

# COMMAND ----------

# DBTITLE 1,Apply VMI customer rules
vmi_customers_df = (
    silver_vmi_customers_df
    .select("customer")
    .dropDuplicates()
    .withColumn("is_vmi_customer", F.lit(True))
)

vmi_restricted_plants_df = (
    silver_vmi_customers_df
    .select(
        F.col("plant_code").alias("candidate_plant_code")
    )
    .dropDuplicates()
    .withColumn("is_candidate_plant_vmi_restricted", F.lit(True))
)

vmi_allowed_plant_customer_df = (
    silver_vmi_customers_df
    .select(
        F.col("plant_code").alias("candidate_plant_code"),
        "customer"
    )
    .dropDuplicates()
    .withColumn("is_allowed_vmi_pair", F.lit(True))
)

shipping_options_with_vmi_df = (
    shipping_options_with_warehouse_df
    .join(
        vmi_customers_df,
        on="customer",
        how="left"
    )
    .withColumn(
        "is_vmi_customer",
        F.coalesce(F.col("is_vmi_customer"), F.lit(False))
    )
    .join(
        vmi_restricted_plants_df,
        on="candidate_plant_code",
        how="left"
    )
    .withColumn(
        "is_candidate_plant_vmi_restricted",
        F.coalesce(F.col("is_candidate_plant_vmi_restricted"), F.lit(False))
    )
    .join(
        vmi_allowed_plant_customer_df,
        on=["candidate_plant_code", "customer"],
        how="left"
    )
    .filter(
        (F.col("is_candidate_plant_vmi_restricted") == False) |
        (F.col("is_allowed_vmi_pair") == True)
    )
    .drop("is_allowed_vmi_pair")
)

print(f"Shipping options after VMI rules rows: {shipping_options_with_vmi_df.count()}")

display(shipping_options_with_vmi_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Calculate logistics cost per valid option
# MAGIC
# MAGIC This step calculates the estimated logistics cost for each valid shipping option.
# MAGIC
# MAGIC The freight cost uses the greater value between:
# MAGIC
# MAGIC - The minimum freight cost.
# MAGIC - The weight multiplied by the freight rate.
# MAGIC
# MAGIC The total logistics cost combines freight cost and warehouse handling cost.

# COMMAND ----------

# DBTITLE 1,Calculate logistics cost per valid option
shipping_options_with_cost_df = (
    shipping_options_with_vmi_df
    .withColumn(
        "freight_cost",
        F.greatest(
            F.col("freight_min_cost"),
            F.col("weight") * F.col("freight_rate")
        )
    )
    .withColumn(
        "warehouse_handling_cost",
        F.col("unit_quant") * F.col("warehouse_cost_per_unit")
    )
    .withColumn(
        "total_logistics_cost",
        F.col("freight_cost") + F.col("warehouse_handling_cost")
    )
)

display(shipping_options_with_cost_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Flag the historical shipping option
# MAGIC
# MAGIC This step identifies whether a candidate option matches the historical route used in the order.
# MAGIC
# MAGIC The flag will be useful later to compare historical cost against optimized cost.

# COMMAND ----------

# DBTITLE 1,Flag the historical shipping option
shipping_options_flagged_df = (
    shipping_options_with_cost_df
    .withColumn(
        "is_historical_option",
        (
            (F.col("candidate_plant_code") == F.col("historical_plant_code")) &
            (F.col("candidate_orig_port") == F.col("historical_orig_port")) &
            (F.col("candidate_carrier") == F.col("historical_carrier"))
        )
    )
)

historical_option_count = (
    shipping_options_flagged_df
    .filter(F.col("is_historical_option") == True)
    .count()
)

print(f"Historical option rows found: {historical_option_count}")
display(shipping_options_flagged_df.filter(F.col("is_historical_option") == True).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 - Create the final valid shipping options DataFrame
# MAGIC
# MAGIC This step defines the final schema for the Silver `valid_shipping_options` table.
# MAGIC
# MAGIC The table includes all valid logistics options for each order after applying product-plant, plant-port, freight rate, warehouse cost, warehouse order capacity and VMI rules.
# MAGIC
# MAGIC No Bronze technical metadata is included because this table is derived from multiple Silver tables.

# COMMAND ----------

# DBTITLE 1,Create the final valid shipping options DataFrame
valid_shipping_options_df = (
    shipping_options_flagged_df
    .select(
        "order_id",
        "order_date",
        "customer",
        "product_id",
        "dest_port",
        "unit_quant",
        "weight",
        "service_level",
        "ship_ahead_day_count",
        "ship_late_day_count",
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
    .dropDuplicates()
)

print(f"Final valid shipping options rows: {valid_shipping_options_df.count()}")

valid_shipping_options_df.printSchema()

display(valid_shipping_options_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 - Validate that every order has at least one valid shipping option
# MAGIC
# MAGIC This step checks whether any Silver order was left without a valid shipping option.
# MAGIC
# MAGIC An order without valid options cannot be optimized, so the notebook stops if any are found.

# COMMAND ----------

# DBTITLE 1,Validate that every order has at least one valid shipping option
orders_with_options_df = (
    valid_shipping_options_df
    .select("order_id")
    .dropDuplicates()
)

orders_without_options_df = (
    orders_base_df
    .select("order_id")
    .dropDuplicates()
    .join(
        orders_with_options_df,
        on="order_id",
        how="left_anti"
    )
)

orders_without_options_count = orders_without_options_df.count()

print(f"Orders without valid shipping options: {orders_without_options_count}")
display(orders_without_options_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14 - Review valid option counts by order
# MAGIC
# MAGIC This step summarizes how many valid options each order has.
# MAGIC
# MAGIC This validation helps confirm that the table is useful for optimization because each order should have one or more alternatives to compare.

# COMMAND ----------

# DBTITLE 1,Review valid option counts by order
option_count_by_order_df = (
    valid_shipping_options_df
    .groupBy("order_id")
    .agg(
        F.count("*").alias("valid_option_count"),
        F.min("total_logistics_cost").alias("minimum_option_cost"),
        F.max("total_logistics_cost").alias("maximum_option_cost")
    )
)

display(option_count_by_order_df.orderBy(F.col("valid_option_count").asc()).limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15 - Review historical option coverage
# MAGIC
# MAGIC This step checks how many orders have a candidate option that matches the historical route.
# MAGIC
# MAGIC This is useful for the future cost comparison, but the notebook does not stop here because the optimization layer may still be built from valid candidate options.

# COMMAND ----------

# DBTITLE 1,Review historical option coverage
orders_with_historical_option_df = (
    valid_shipping_options_df
    .filter(F.col("is_historical_option") == True)
    .select("order_id")
    .dropDuplicates()
)

orders_without_historical_option_df = (
    orders_base_df
    .select("order_id")
    .dropDuplicates()
    .join(
        orders_with_historical_option_df,
        on="order_id",
        how="left_anti"
    )
)

orders_without_historical_option_count = orders_without_historical_option_df.count()

print(f"Orders without historical option match: {orders_without_historical_option_count}")
display(orders_without_historical_option_df.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 16 - Write the Silver valid shipping options table
# MAGIC
# MAGIC This step writes the final valid shipping options DataFrame as an external Delta table.
# MAGIC
# MAGIC The logical table is registered in Unity Catalog as `az_supplychain.silver.valid_shipping_options`.
# MAGIC
# MAGIC The physical data is stored under the Silver ADLS path.
# MAGIC
# MAGIC `overwrite` is used because this derived table is rebuilt completely from the current Silver inputs.

# COMMAND ----------

# DBTITLE 1,Write the Silver valid shipping options table
target_table = SILVER_TABLES["valid_shipping_options"]
target_path = SILVER_TABLE_PATHS["valid_shipping_options"]

(
    valid_shipping_options_df.write
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
# MAGIC ## Step 17 - Validate the written Silver table
# MAGIC
# MAGIC This step reads the table back from Unity Catalog and confirms that it was written correctly.
# MAGIC
# MAGIC It also displays the Delta table metadata to verify the physical ADLS location.

# COMMAND ----------

# DBTITLE 1,Validate the written Silver table
valid_shipping_options_table_df = spark.table(SILVER_TABLES["valid_shipping_options"])

print(f"Final valid shipping options row count: {valid_shipping_options_table_df.count()}")

display(valid_shipping_options_table_df.limit(10))
display(spark.sql(f"DESCRIBE DETAIL {SILVER_TABLES['valid_shipping_options']}"))