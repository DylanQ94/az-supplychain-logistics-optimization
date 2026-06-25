# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 05_silver_to_gold_dimensions
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook creates the Gold dimension tables for the Az-SupplyChain analytical model.
# MAGIC
# MAGIC The dimensions are built from trusted Silver tables and are designed to support Synapse views, Power BI relationships and analytical filtering.
# MAGIC
# MAGIC ## Inputs
# MAGIC
# MAGIC Silver tables:
# MAGIC
# MAGIC - `az_supplychain.silver.order_list`
# MAGIC - `az_supplychain.silver.valid_shipping_options`
# MAGIC - `az_supplychain.silver.freight_rates`
# MAGIC - `az_supplychain.silver.wh_costs`
# MAGIC - `az_supplychain.silver.wh_capacities`
# MAGIC - `az_supplychain.silver.vmi_customers`
# MAGIC - `az_supplychain.silver.plant_ports`
# MAGIC - `az_supplychain.silver.products_per_plant`
# MAGIC
# MAGIC ## Outputs
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
# MAGIC ## Processing flow
# MAGIC
# MAGIC 1. Load shared project configuration from `00_config`.
# MAGIC 2. Read the required Silver tables.
# MAGIC 3. Create the Date dimension.
# MAGIC 4. Create Product and Customer dimensions.
# MAGIC 5. Create Plant and Port dimensions.
# MAGIC 6. Validate carrier type consistency.
# MAGIC 7. Create Carrier dimension.
# MAGIC 8. Create Transport Mode and Service Level dimensions.
# MAGIC 9. Validate dimension keys.
# MAGIC 10. Write Gold dimensions as external Delta tables.
# MAGIC 11. Validate the written Gold dimension tables.
# MAGIC
# MAGIC ## Important notes
# MAGIC
# MAGIC - `dim_order` is not created in this version because `order_id` will be handled as a degenerate dimension in the facts.
# MAGIC - No artificial surrogate keys are created except `date_key` for the Date dimension.
# MAGIC - No unnecessary descriptive columns are invented.
# MAGIC - The Gold dimensions are intentionally simple and focused on analytical value.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load project configuration
# MAGIC
# MAGIC This step imports the shared configuration from `00_config`.
# MAGIC
# MAGIC The configuration contains Unity Catalog names, ADLS paths and base table locations.

# COMMAND ----------

# DBTITLE 1,Load project configuration
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Import PySpark functions
# MAGIC
# MAGIC This step imports the PySpark functions needed to build dimensions.
# MAGIC
# MAGIC The main functions used are:
# MAGIC
# MAGIC - `col()` to reference columns clearly.
# MAGIC - `lit()` to create controlled flags.
# MAGIC - `coalesce()` to handle missing VMI flags.
# MAGIC - `date_format()`, `year()`, `quarter()` and `month()` to build the Date dimension.
# MAGIC - `dropDuplicates()` to keep one row per dimension member.

# COMMAND ----------

# DBTITLE 1,Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Read required Silver tables
# MAGIC
# MAGIC This step reads the Silver tables needed to build the Gold dimensions.
# MAGIC
# MAGIC `spark.table()` is used because all input tables are already registered in Unity Catalog.

# COMMAND ----------

# DBTITLE 1,Read required Silver tables
silver_orders_df = spark.table(SILVER_TABLES["order_list"])
valid_shipping_options_df = spark.table(SILVER_TABLES["valid_shipping_options"])
silver_freight_rates_df = spark.table(SILVER_TABLES["freight_rates"])
silver_wh_costs_df = spark.table(SILVER_TABLES["wh_costs"])
silver_wh_capacities_df = spark.table(SILVER_TABLES["wh_capacities"])
silver_vmi_customers_df = spark.table(SILVER_TABLES["vmi_customers"])
silver_plant_ports_df = spark.table(SILVER_TABLES["plant_ports"])
silver_products_per_plant_df = spark.table(SILVER_TABLES["products_per_plant"])

print(f"Silver orders rows:                 {silver_orders_df.count()}")
print(f"Valid shipping options rows:        {valid_shipping_options_df.count()}")
print(f"Silver freight rates rows:          {silver_freight_rates_df.count()}")
print(f"Silver warehouse costs rows:        {silver_wh_costs_df.count()}")
print(f"Silver warehouse capacities rows:   {silver_wh_capacities_df.count()}")
print(f"Silver VMI customers rows:          {silver_vmi_customers_df.count()}")
print(f"Silver plant ports rows:            {silver_plant_ports_df.count()}")
print(f"Silver products per plant rows:     {silver_products_per_plant_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Create the Date dimension
# MAGIC
# MAGIC This step creates `dim_date` from the order dates available in Silver.
# MAGIC
# MAGIC The Date dimension supports temporal filtering and aggregation in Synapse and Power BI.

# COMMAND ----------

# DBTITLE 1,Create the Date dimension
date_base_df = (
    silver_orders_df
    .select("order_date")
    .union(valid_shipping_options_df.select("order_date"))
    .filter(F.col("order_date").isNotNull())
    .dropDuplicates()
)

dim_date_df = (
    date_base_df
    .select(
        F.date_format(F.col("order_date"), "yyyyMMdd").cast("int").alias("date_key"),
        F.col("order_date"),
        F.year(F.col("order_date")).alias("calendar_year"),
        F.quarter(F.col("order_date")).alias("calendar_quarter"),
        F.month(F.col("order_date")).alias("month_number"),
        F.date_format(F.col("order_date"), "MMMM").alias("month_name"),
        F.date_format(F.col("order_date"), "yyyy-MM").alias("year_month")
    )
    .orderBy("order_date")
)

print(f"Date dimension rows: {dim_date_df.count()}")
display(dim_date_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Create the Product dimension
# MAGIC
# MAGIC This step creates `dim_product`.
# MAGIC
# MAGIC The dataset only provides the product identifier, so the dimension is currently thin.
# MAGIC
# MAGIC It is still kept because products are important analytical entities for filtering, relationships and future model extensibility.

# COMMAND ----------

# DBTITLE 1,Create the Product dimension
dim_product_df = (
    silver_orders_df.select("product_id")
    .union(valid_shipping_options_df.select("product_id"))
    .union(silver_products_per_plant_df.select("product_id"))
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates()
    .orderBy("product_id")
)

print(f"Product dimension rows: {dim_product_df.count()}")
display(dim_product_df.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Create the Customer dimension
# MAGIC
# MAGIC This step creates `dim_customer`.
# MAGIC
# MAGIC The `is_vmi_customer` flag is added because VMI customers follow special logistics rules and are useful for analytical segmentation.

# COMMAND ----------

# DBTITLE 1,Create the Customer dimension
customer_base_df = (
    silver_orders_df.select("customer")
    .union(valid_shipping_options_df.select("customer"))
    .union(silver_vmi_customers_df.select("customer"))
    .filter(F.col("customer").isNotNull())
    .dropDuplicates()
)

vmi_customer_flags_df = (
    silver_vmi_customers_df
    .select("customer")
    .dropDuplicates()
    .withColumn("is_vmi_customer", F.lit(True))
)

dim_customer_df = (
    customer_base_df
    .join(
        vmi_customer_flags_df,
        on="customer",
        how="left"
    )
    .withColumn(
        "is_vmi_customer",
        F.coalesce(F.col("is_vmi_customer"), F.lit(False))
    )
    .orderBy("customer")
)

print(f"Customer dimension rows: {dim_customer_df.count()}")
display(dim_customer_df.limit(50))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Create the Plant dimension
# MAGIC
# MAGIC This step creates `dim_plant`.
# MAGIC
# MAGIC The Plant dimension combines plant codes with warehouse cost and daily order capacity attributes.
# MAGIC
# MAGIC `cost_per_unit` is used for warehouse handling cost analysis.
# MAGIC
# MAGIC `daily_order_capacity` represents the maximum number of orders that each plant can process per day. It does not represent product units or weight capacity.
# MAGIC
# MAGIC These attributes are useful for capacity analysis, cost analysis and optimization interpretation.

# COMMAND ----------

# DBTITLE 1,Create the Plant dimension
plant_base_df = (
    valid_shipping_options_df
    .select(F.col("candidate_plant_code").alias("plant_code"))
    .union(valid_shipping_options_df.select(F.col("historical_plant_code").alias("plant_code")))
    .union(silver_wh_costs_df.select("plant_code"))
    .union(silver_wh_capacities_df.select("plant_code"))
    .union(silver_plant_ports_df.select("plant_code"))
    .union(silver_products_per_plant_df.select("plant_code"))
    .filter(F.col("plant_code").isNotNull())
    .dropDuplicates()
)

plant_cost_df = (
    silver_wh_costs_df
    .select(
        "plant_code",
        "cost_per_unit"
    )
)

plant_capacity_df = (
    silver_wh_capacities_df
    .select(
        "plant_code",
        "daily_order_capacity"
    )
)

dim_plant_df = (
    plant_base_df
    .join(
        plant_cost_df,
        on="plant_code",
        how="left"
    )
    .join(
        plant_capacity_df,
        on="plant_code",
        how="left"
    )
)

print(f"Plant dimension rows: {dim_plant_df.count()}")

display(dim_plant_df.orderBy("plant_code"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Create the Port dimension
# MAGIC
# MAGIC This step creates `dim_port`.
# MAGIC
# MAGIC A port can appear as an origin port, a destination port or both.
# MAGIC
# MAGIC The role flags help analyze whether a port is used as an origin, destination or both in the logistics network.

# COMMAND ----------

# DBTITLE 1,Create the Port dimension
origin_ports_df = (
    valid_shipping_options_df
    .select(F.col("candidate_orig_port").alias("port"))
    .union(valid_shipping_options_df.select(F.col("historical_orig_port").alias("port")))
    .filter(F.col("port").isNotNull())
    .dropDuplicates()
    .withColumn("is_origin_port", F.lit(True))
    .withColumn("is_destination_port", F.lit(False))
)

destination_ports_df = (
    valid_shipping_options_df
    .select(F.col("dest_port").alias("port"))
    .filter(F.col("port").isNotNull())
    .dropDuplicates()
    .withColumn("is_origin_port", F.lit(False))
    .withColumn("is_destination_port", F.lit(True))
)

dim_port_df = (
    origin_ports_df
    .unionByName(destination_ports_df)
    .groupBy("port")
    .agg(
        F.max(F.col("is_origin_port").cast("int")).cast("boolean").alias("is_origin_port"),
        F.max(F.col("is_destination_port").cast("int")).cast("boolean").alias("is_destination_port")
    )
)

print(f"Port dimension rows: {dim_port_df.count()}")
display(dim_port_df.orderBy("port"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Validate carrier type consistency
# MAGIC
# MAGIC This step validates whether each carrier has only one carrier type.
# MAGIC
# MAGIC This validation matters because `dim_carrier` will use `carrier` as the relationship key.
# MAGIC
# MAGIC If a carrier has multiple carrier types, the dimension grain would need to change.

# COMMAND ----------

# DBTITLE 1,Validate carrier type consistency
carrier_type_validation_df = (
    silver_freight_rates_df
    .select("carrier", "carrier_type")
    .dropDuplicates()
    .groupBy("carrier")
    .count()
    .filter(F.col("count") > 1)
)

carrier_type_conflict_count = carrier_type_validation_df.count()

display(carrier_type_validation_df)

if carrier_type_conflict_count > 0:
    raise ValueError(
        f"Carrier type consistency validation failed. "
        f"Carriers with multiple carrier types found: {carrier_type_conflict_count}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Create the Carrier dimension
# MAGIC
# MAGIC This step creates `dim_carrier`.
# MAGIC
# MAGIC The Carrier dimension supports analysis by transport provider and carrier type.

# COMMAND ----------

# DBTITLE 1,Create the Carrier dimension
carrier_base_df = (
    silver_orders_df
    .select("carrier")
    .union(valid_shipping_options_df.select(F.col("candidate_carrier").alias("carrier")))
    .union(silver_freight_rates_df.select("carrier"))
    .filter(F.col("carrier").isNotNull())
    .dropDuplicates()
)

carrier_type_df = (
    silver_freight_rates_df
    .select("carrier", "carrier_type")
    .dropDuplicates()
)

dim_carrier_df = (
    carrier_base_df
    .join(
        carrier_type_df,
        on="carrier",
        how="left"
    )
)

print(f"Carrier dimension rows: {dim_carrier_df.count()}")
display(dim_carrier_df.orderBy("carrier"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Create the Transport Mode dimension
# MAGIC
# MAGIC This step creates `dim_transport_mode`.
# MAGIC
# MAGIC The dimension is intentionally simple, but it is useful for model relationships, filtering and future extension with additional transport attributes.

# COMMAND ----------

# DBTITLE 1,Create the Transport Mode dimension
dim_transport_mode_df = (
    valid_shipping_options_df
    .select(F.col("candidate_transport_mode").alias("transport_mode"))
    .union(silver_freight_rates_df.select(F.col("mode_dsc").alias("transport_mode")))
    .filter(F.col("transport_mode").isNotNull())
    .dropDuplicates()
)

print(f"Transport mode dimension rows: {dim_transport_mode_df.count()}")
display(dim_transport_mode_df.orderBy("transport_mode"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 - Create the Service Level dimension
# MAGIC
# MAGIC This step creates `dim_service_level`.
# MAGIC
# MAGIC Service level is important because valid shipping options preserve the original service level requirement of each order.
# MAGIC
# MAGIC Keeping it as a dimension makes the Gold model clearer for Synapse and Power BI.

# COMMAND ----------

# DBTITLE 1,Create the Service Level dimension
dim_service_level_df = (
    silver_orders_df.select("service_level")
    .union(valid_shipping_options_df.select("service_level"))
    .union(silver_freight_rates_df.select("service_level"))
    .filter(F.col("service_level").isNotNull())
    .dropDuplicates()
)

print(f"Service level dimension rows: {dim_service_level_df.count()}")
display(dim_service_level_df.orderBy("service_level"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 - Validate dimension keys
# MAGIC
# MAGIC This step validates that every dimension has non-null and unique keys.
# MAGIC
# MAGIC These checks are important because dimensions will be used for relationships in Synapse and Power BI.

# COMMAND ----------

# DBTITLE 1,Validate dimension keys
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

failed_dimensions = (
    dimension_validation_df
    .filter(
        (F.col("null_key_count") > 0) |
        (F.col("duplicate_key_count") > 0)
    )
    .collect()
)

if failed_dimensions:
    raise ValueError("Dimension key validation failed. Review dimension_validation_df.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 14 - Write Gold dimension tables
# MAGIC
# MAGIC This step writes the Gold dimensions as external Delta tables registered in Unity Catalog.
# MAGIC
# MAGIC The logical table names and physical paths come from `00_config`.
# MAGIC
# MAGIC `overwrite` is used because these dimensions are rebuilt completely from the trusted Silver layer.

# COMMAND ----------

# DBTITLE 1,Write Gold dimension tables
gold_dimensions_to_write = {
    "dim_date": dim_date_df,
    "dim_product": dim_product_df,
    "dim_customer": dim_customer_df,
    "dim_plant": dim_plant_df,
    "dim_port": dim_port_df,
    "dim_carrier": dim_carrier_df,
    "dim_transport_mode": dim_transport_mode_df,
    "dim_service_level": dim_service_level_df
}

for dimension_name, dataframe in gold_dimensions_to_write.items():
    target_table = GOLD_TABLES[dimension_name]
    target_path = GOLD_TABLE_PATHS[dimension_name]

    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", target_path)
        .saveAsTable(target_table)
    )

    print(f"Written Gold dimension: {target_table}")
    print(f"Physical path: {target_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15 - Validate written Gold dimensions
# MAGIC
# MAGIC This step reads each Gold dimension back from Unity Catalog and returns the final row counts.
# MAGIC
# MAGIC This confirms that all dimension tables were written and registered correctly.

# COMMAND ----------

# DBTITLE 1,Validate written Gold dimensions
gold_dimension_validation = []

for dimension_name in gold_dimensions_to_write.keys():
    target_table = GOLD_TABLES[dimension_name]
    target_path = GOLD_TABLE_PATHS[dimension_name]
    row_count = spark.table(target_table).count()

    gold_dimension_validation.append({
        "dimension_name": dimension_name,
        "unity_catalog_table": target_table,
        "physical_path": target_path,
        "row_count": row_count
    })

gold_dimension_validation_df = spark.createDataFrame(gold_dimension_validation)

display(gold_dimension_validation_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 16 - List tables in the Gold schema
# MAGIC
# MAGIC This final step lists the tables currently available in `az_supplychain.gold`.
# MAGIC
# MAGIC At this point, the Gold schema should contain the analytical dimensions created by this notebook.

# COMMAND ----------

# DBTITLE 1,List tables in the Gold schema
display(spark.sql(f"SHOW TABLES IN {UC_GOLD}"))

# COMMAND ----------

# MAGIC %md
# MAGIC