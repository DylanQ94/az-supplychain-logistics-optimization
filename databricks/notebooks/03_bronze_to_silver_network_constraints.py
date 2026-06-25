# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03_bronze_to_silver_network_constraints
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook transforms the Bronze logistics constraint tables into clean, typed and validated Silver tables.
# MAGIC
# MAGIC These Silver tables will be used later to build valid shipping options, analytical Gold tables and the optimization layer.
# MAGIC
# MAGIC ## Inputs
# MAGIC
# MAGIC Bronze tables:
# MAGIC
# MAGIC - `az_supplychain.bronze.freight_rates`
# MAGIC - `az_supplychain.bronze.plant_ports`
# MAGIC - `az_supplychain.bronze.products_per_plant`
# MAGIC - `az_supplychain.bronze.wh_costs`
# MAGIC - `az_supplychain.bronze.wh_capacities`
# MAGIC - `az_supplychain.bronze.vmi_customers`
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC Silver tables:
# MAGIC
# MAGIC - `az_supplychain.silver.freight_rates`
# MAGIC - `az_supplychain.silver.plant_ports`
# MAGIC - `az_supplychain.silver.products_per_plant`
# MAGIC - `az_supplychain.silver.wh_costs`
# MAGIC - `az_supplychain.silver.wh_capacities`
# MAGIC - `az_supplychain.silver.vmi_customers`
# MAGIC
# MAGIC ## Processing flow
# MAGIC
# MAGIC 1. Load shared project configuration from `00_config`.
# MAGIC 2. Read the Bronze logistics constraint tables.
# MAGIC 3. Clean and type freight rates.
# MAGIC 4. Clean and deduplicate plant-port relationships.
# MAGIC 5. Clean and deduplicate product-plant relationships.
# MAGIC 6. Clean and validate warehouse costs.
# MAGIC 7. Clean and validate warehouse capacities.
# MAGIC 8. Clean and deduplicate VMI customer rules.
# MAGIC 9. Run quality validations with real business value.
# MAGIC 10. Write the Silver tables as external Delta tables registered in Unity Catalog.
# MAGIC 11. Validate the final Silver tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load project configuration
# MAGIC
# MAGIC This step imports the shared configuration from `00_config`.
# MAGIC
# MAGIC The configuration contains Unity Catalog table names and the physical ADLS paths where the Silver Delta tables will be stored.

# COMMAND ----------

# DBTITLE 1,Load project configuration
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Import PySpark functions
# MAGIC
# MAGIC This step imports the PySpark functions needed for cleaning, type conversion and validation.
# MAGIC
# MAGIC The main functions used are:
# MAGIC
# MAGIC - `trim()` to remove leading and trailing spaces.
# MAGIC - `upper()` to standardize business codes used in joins.
# MAGIC - `cast()` to convert numeric fields to the correct types.
# MAGIC - `col()` to reference DataFrame columns clearly.

# COMMAND ----------

# DBTITLE 1,Import PySpark functions
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Read Bronze logistics constraint tables
# MAGIC
# MAGIC This step reads the Bronze tables from Unity Catalog.
# MAGIC
# MAGIC `spark.table()` is used because the tables are already registered in Unity Catalog. This keeps the notebook governed and avoids reading directly from raw ADLS paths.

# COMMAND ----------

# DBTITLE 1,Read Bronze logistics constraint tables
bronze_freight_rates_df = spark.table(BRONZE_TABLES["freight_rates"])
bronze_plant_ports_df = spark.table(BRONZE_TABLES["plant_ports"])
bronze_products_per_plant_df = spark.table(BRONZE_TABLES["products_per_plant"])
bronze_wh_costs_df = spark.table(BRONZE_TABLES["wh_costs"])
bronze_wh_capacities_df = spark.table(BRONZE_TABLES["wh_capacities"])
bronze_vmi_customers_df = spark.table(BRONZE_TABLES["vmi_customers"])

print(f"Bronze freight_rates rows:        {bronze_freight_rates_df.count()}")
print(f"Bronze plant_ports rows:          {bronze_plant_ports_df.count()}")
print(f"Bronze products_per_plant rows:   {bronze_products_per_plant_df.count()}")
print(f"Bronze wh_costs rows:             {bronze_wh_costs_df.count()}")
print(f"Bronze wh_capacities rows:        {bronze_wh_capacities_df.count()}")
print(f"Bronze vmi_customers rows:        {bronze_vmi_customers_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Clean and type freight rates
# MAGIC
# MAGIC This step prepares the freight rates table for future matching with orders.
# MAGIC
# MAGIC Business codes are standardized with `trim()` and `upper()` because they will be used later to join orders with valid carrier, port, service and weight-range options.
# MAGIC
# MAGIC Numeric fields are cast to numeric types because they will be used later for freight cost calculation.

# COMMAND ----------

# DBTITLE 1,Clean and type freight rates
silver_freight_rates_df = (
    bronze_freight_rates_df
    .select(
        "carrier",
        "orig_port",
        "dest_port",
        "min_weight_quant",
        "max_weight_quant",
        "service_level",
        "min_cost",
        "rate",
        "mode_dsc",
        "tpt_day_count",
        "carrier_type",
        "source_file_path",
        "ingestion_timestamp"
    )
    .withColumn("carrier", F.upper(F.trim(F.col("carrier").cast("string"))))
    .withColumn("orig_port", F.upper(F.trim(F.col("orig_port").cast("string"))))
    .withColumn("dest_port", F.upper(F.trim(F.col("dest_port").cast("string"))))
    .withColumn("service_level", F.upper(F.trim(F.col("service_level").cast("string"))))
    .withColumn("mode_dsc", F.upper(F.trim(F.col("mode_dsc").cast("string"))))
    .withColumn("carrier_type", F.upper(F.trim(F.col("carrier_type").cast("string"))))
    .withColumn("min_weight_quant", F.col("min_weight_quant").cast("double"))
    .withColumn("max_weight_quant", F.col("max_weight_quant").cast("double"))
    .withColumn("min_cost", F.col("min_cost").cast("double"))
    .withColumn("rate", F.col("rate").cast("double"))
    .withColumn("tpt_day_count", F.col("tpt_day_count").cast("int"))
    .dropDuplicates()
)

silver_freight_rates_df.printSchema()
display(silver_freight_rates_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Clean plant-port relationships
# MAGIC
# MAGIC This step prepares the relationship between plants and ports.
# MAGIC
# MAGIC Exact duplicate relationships are removed because they do not add information and would multiply rows when building valid shipping options.

# COMMAND ----------

# DBTITLE 1,Clean plant-port relationships
silver_plant_ports_df = (
    bronze_plant_ports_df
    .select(
        "plant_code",
        "port",
        "source_file_path",
        "ingestion_timestamp"
    )
    .withColumn("plant_code", F.upper(F.trim(F.col("plant_code").cast("string"))))
    .withColumn("port", F.upper(F.trim(F.col("port").cast("string"))))
    .dropDuplicates(["plant_code", "port"])
)

display(silver_plant_ports_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Clean product-plant relationships
# MAGIC
# MAGIC This step prepares the list of products that each plant can handle.
# MAGIC
# MAGIC Exact duplicate relationships are removed because they do not add information and can duplicate valid shipping options later.

# COMMAND ----------

# DBTITLE 1,Clean product-plant relationships
silver_products_per_plant_df = (
    bronze_products_per_plant_df
    .select(
        "plant_code",
        "product_id",
        "source_file_path",
        "ingestion_timestamp"
    )
    .withColumn("plant_code", F.upper(F.trim(F.col("plant_code").cast("string"))))
    .withColumn("product_id", F.upper(F.trim(F.col("product_id").cast("string"))))
    .dropDuplicates(["plant_code", "product_id"])
)

display(silver_products_per_plant_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Clean warehouse costs
# MAGIC
# MAGIC This step prepares warehouse unit costs.
# MAGIC
# MAGIC `plant_code` is standardized because it will be joined with orders and plant constraints.
# MAGIC
# MAGIC `cost_per_unit` is cast to double because it will be used later to calculate warehouse handling cost.

# COMMAND ----------

# DBTITLE 1,Clean warehouse costs
silver_wh_costs_df = (
    bronze_wh_costs_df
    .select(
        "plant_code",
        "cost_per_unit",
        "source_file_path",
        "ingestion_timestamp"
    )
    .withColumn("plant_code", F.upper(F.trim(F.col("plant_code").cast("string"))))
    .withColumn("cost_per_unit", F.col("cost_per_unit").cast("double"))
)

display(silver_wh_costs_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Clean warehouse order capacities
# MAGIC
# MAGIC This step prepares warehouse capacity data.
# MAGIC
# MAGIC According to the dataset definition, `WhCapacities` represents the maximum number of orders that each plant can process per day.
# MAGIC
# MAGIC It does not represent product units, weight or product-specific capacity.

# COMMAND ----------

# DBTITLE 1,Clean warehouse capacities
silver_wh_capacities_df = (
    bronze_wh_capacities_df
    .select(
        "plant_code",
        "daily_capacity",
        "source_file_path",
        "ingestion_timestamp"
    )
    .withColumn("plant_code", F.upper(F.trim(F.col("plant_code").cast("string"))))
    .withColumn("daily_order_capacity", F.col("daily_capacity").cast("int"))
    .drop("daily_capacity")
)

display(silver_wh_capacities_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Clean VMI customer rules
# MAGIC
# MAGIC This step prepares the customer-plant rules for VMI customers.
# MAGIC
# MAGIC Exact duplicate rules are removed because they do not add information and can duplicate valid options later.

# COMMAND ----------

# DBTITLE 1,Clean VMI customer rules
silver_vmi_customers_df = (
    bronze_vmi_customers_df
    .select(
        "plant_code",
        "customer",
        "source_file_path",
        "ingestion_timestamp"
    )
    .withColumn("plant_code", F.upper(F.trim(F.col("plant_code").cast("string"))))
    .withColumn("customer", F.upper(F.trim(F.col("customer").cast("string"))))
    .dropDuplicates(["plant_code", "customer"])
)

display(silver_vmi_customers_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 - Validate required fields
# MAGIC
# MAGIC This step validates that key fields are not null.
# MAGIC
# MAGIC These fields are required because they will be used later to build valid shipping options and to calculate logistics costs.

# COMMAND ----------

# DBTITLE 1,Validate required fields
required_field_checks = [
    ("freight_rates", silver_freight_rates_df, [
        "carrier",
        "orig_port",
        "dest_port",
        "min_weight_quant",
        "max_weight_quant",
        "service_level",
        "min_cost",
        "rate",
        "mode_dsc",
        "tpt_day_count"
    ]),
    ("plant_ports", silver_plant_ports_df, [
        "plant_code",
        "port"
    ]),
    ("products_per_plant", silver_products_per_plant_df, [
        "plant_code",
        "product_id"
    ]),
    ("wh_costs", silver_wh_costs_df, [
        "plant_code",
        "cost_per_unit"
    ]),
    ("wh_capacities", silver_wh_capacities_df, [
    "plant_code",
    "daily_order_capacity"
    ]),
    ("vmi_customers", silver_vmi_customers_df, [
        "plant_code",
        "customer"
    ])
]

null_validation_results = []

for table_name, dataframe, required_columns in required_field_checks:
    for column_name in required_columns:
        null_count = dataframe.filter(F.col(column_name).isNull()).count()

        null_validation_results.append({
            "table_name": table_name,
            "column_name": column_name,
            "null_count": null_count
        })

null_validation_df = spark.createDataFrame(null_validation_results)

display(null_validation_df)

columns_with_nulls = (
    null_validation_df
    .filter(F.col("null_count") > 0)
    .collect()
)

if columns_with_nulls:
    raise ValueError("Required null validation failed. Review null_validation_df.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11 - Validate numeric business rules
# MAGIC
# MAGIC This step validates numeric fields with business meaning.
# MAGIC
# MAGIC Freight weight ranges, costs, rates and warehouse capacities must be valid before they can be used in downstream cost calculations and optimization.

# COMMAND ----------

# DBTITLE 1,Validate numeric business rules
 invalid_freight_rates_df = silver_freight_rates_df.filter(
    (F.col("min_weight_quant") < 0) |
    (F.col("max_weight_quant") <= F.col("min_weight_quant")) |
    (F.col("min_cost") < 0) |
    (F.col("rate") < 0) |
    (F.col("tpt_day_count") < 0)
)

invalid_wh_costs_df = silver_wh_costs_df.filter(
    F.col("cost_per_unit") < 0
)

invalid_wh_capacities_df = silver_wh_capacities_df.filter(
    F.col("daily_order_capacity") <= 0
)

invalid_freight_rates_count = invalid_freight_rates_df.count()
invalid_wh_costs_count = invalid_wh_costs_df.count()
invalid_wh_capacities_count = invalid_wh_capacities_df.count()

print(f"Invalid freight rate rows:     {invalid_freight_rates_count}")
print(f"Invalid warehouse cost rows:   {invalid_wh_costs_count}")
print(f"Invalid capacity rows:         {invalid_wh_capacities_count}")

display(invalid_freight_rates_df.limit(50))
display(invalid_wh_costs_df.limit(50))
display(invalid_wh_capacities_df.limit(50))

if invalid_freight_rates_count > 0:
    print(f"Invalid freight rate records found: {invalid_freight_rates_count}")

if invalid_wh_costs_count > 0:
    print(f"Invalid warehouse cost records found: {invalid_wh_costs_count}")

if invalid_wh_capacities_count > 0:
    print(f"Invalid warehouse capacity records found: {invalid_wh_capacities_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 12 - Validate uniqueness for warehouse cost and order capacity tables
# MAGIC
# MAGIC This step validates that each plant has only one warehouse cost and one daily order capacity value.
# MAGIC
# MAGIC Duplicate plant records in these tables would make downstream cost and capacity calculations ambiguous.

# COMMAND ----------

# DBTITLE 1,Validate uniqueness for warehouse cost and capacity tables
duplicate_wh_costs_df = (
    silver_wh_costs_df
    .groupBy("plant_code")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_wh_capacities_df = (
    silver_wh_capacities_df
    .groupBy("plant_code")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_wh_costs_count = duplicate_wh_costs_df.count()
duplicate_wh_capacities_count = duplicate_wh_capacities_df.count()

display(duplicate_wh_costs_df)
display(duplicate_wh_capacities_df)

if duplicate_wh_costs_count > 0:
    raise ValueError(f"Duplicate warehouse cost records found: {duplicate_wh_costs_count}")

if duplicate_wh_capacities_count > 0:
    raise ValueError(f"Duplicate warehouse order capacity records found: {duplicate_wh_capacities_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 13 - Write Silver logistics constraint tables
# MAGIC
# MAGIC This step writes the cleaned logistics constraint tables as external Delta tables.
# MAGIC
# MAGIC The logical tables are registered in Unity Catalog under `az_supplychain.silver`.
# MAGIC
# MAGIC The physical data is stored in ADLS under the Silver layer.
# MAGIC
# MAGIC `overwrite` is used because these Silver tables are rebuilt completely from the static Bronze source during reprocessing.

# COMMAND ----------

# DBTITLE 1,Write Silver logistics constraint tables
silver_tables_to_write = {
    "freight_rates": silver_freight_rates_df,
    "plant_ports": silver_plant_ports_df,
    "products_per_plant": silver_products_per_plant_df,
    "wh_costs": silver_wh_costs_df,
    "wh_capacities": silver_wh_capacities_df,
    "vmi_customers": silver_vmi_customers_df
}

for table_name, dataframe in silver_tables_to_write.items():
    target_table = SILVER_TABLES[table_name]
    target_path = SILVER_TABLE_PATHS[table_name]

    (
        dataframe.write
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
# MAGIC ## Step 14 - Validate final Silver logistics constraint tables
# MAGIC
# MAGIC This step reads each Silver table back from Unity Catalog and returns the final row count.
# MAGIC
# MAGIC This confirms that the tables were created correctly and are ready for the next notebook.

# COMMAND ----------

# DBTITLE 1,Validate final Silver logistics constraint tables
silver_validation_results = []

for table_name in silver_tables_to_write.keys():
    target_table = SILVER_TABLES[table_name]
    target_path = SILVER_TABLE_PATHS[table_name]
    row_count = spark.table(target_table).count()

    silver_validation_results.append({
        "silver_table": table_name,
        "unity_catalog_table": target_table,
        "physical_path": target_path,
        "row_count": row_count
    })

silver_validation_df = spark.createDataFrame(silver_validation_results)

display(silver_validation_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 15 - List tables in the Silver schema
# MAGIC
# MAGIC This final step lists the tables currently available in `az_supplychain.silver`.
# MAGIC
# MAGIC At this point, Silver should contain the cleaned orders table and the cleaned logistics constraint tables.

# COMMAND ----------

# DBTITLE 1,List tables in the Silver schema
display(spark.sql(f"SHOW TABLES IN {UC_SILVER}"))