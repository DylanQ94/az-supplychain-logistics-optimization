# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 00_config
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook centralizes the configuration used across the Az-SupplyChain Databricks notebooks.
# MAGIC
# MAGIC It defines Unity Catalog objects, ADLS Gen2 paths, external volume paths, and reusable constants for Bronze, Silver, Gold, Optimization and Quality layers.
# MAGIC
# MAGIC ## Inputs
# MAGIC
# MAGIC - Unity Catalog catalog: `az_supplychain`
# MAGIC - Schemas:
# MAGIC   - `bronze`
# MAGIC   - `silver`
# MAGIC   - `gold`
# MAGIC   - `optimization`
# MAGIC   - `quality`
# MAGIC - ADLS Gen2 container: `supply-chain`
# MAGIC - Storage account: `sasupplychainz`
# MAGIC - Landing external volume: `/Volumes/az_supplychain/bronze/landing_files/`
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC This notebook does not write data.
# MAGIC
# MAGIC It exposes reusable Python variables that can be imported by other notebooks using:
# MAGIC
# MAGIC ```python
# MAGIC %run ./00_config
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Define the main project constants.
# MAGIC
# MAGIC These values will be reused by the next notebooks to avoid hardcoding catalog names, schema names, storage account names and base paths.

# COMMAND ----------

# DBTITLE 1,Define the main project constants.
PROJECT_NAME = "az_supplychain"

CATALOG_NAME = "az_supplychain"

SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
SCHEMA_OPTIMIZATION = "optimization"
SCHEMA_QUALITY = "quality"

STORAGE_ACCOUNT_NAME = "sasupplychainz"
CONTAINER_NAME = "supply-chain"

SOURCE_SYSTEM = "figshare"
DATASET_NAME = "supply_chain_logistics"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Define the ADLS Gen2 base paths for each data lake layer.
# MAGIC
# MAGIC These paths point to the physical storage locations that are governed through Unity Catalog external locations.

# COMMAND ----------

# DBTITLE 1,Define the ADLS Gen2 base paths for each data lake layer
ADLS_BASE_PATH = f"abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"

LANDING_BASE_PATH = f"{ADLS_BASE_PATH}/landing/{DATASET_NAME}"
BRONZE_BASE_PATH = f"{ADLS_BASE_PATH}/bronze/{DATASET_NAME}"
SILVER_BASE_PATH = f"{ADLS_BASE_PATH}/silver/{DATASET_NAME}"
GOLD_BASE_PATH = f"{ADLS_BASE_PATH}/gold/{DATASET_NAME}"
OPTIMIZATION_BASE_PATH = f"{ADLS_BASE_PATH}/optimization/{DATASET_NAME}"
QUALITY_BASE_PATH = f"{ADLS_BASE_PATH}/quality/{DATASET_NAME}"
SYNAPSE_MATERIALIZED_BASE_PATH = f"{ADLS_BASE_PATH}/synapse_materialized/{DATASET_NAME}"

LANDING_SOURCE_PATH = f"{LANDING_BASE_PATH}/source={SOURCE_SYSTEM}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 - Define Unity Catalog table namespaces.
# MAGIC
# MAGIC These variables standardize how tables will be referenced in Bronze, Silver, Gold, Optimization and Quality layers.

# COMMAND ----------

# DBTITLE 1,Define Unity Catalog table namespaces
UC_BRONZE = f"{CATALOG_NAME}.{SCHEMA_BRONZE}"
UC_SILVER = f"{CATALOG_NAME}.{SCHEMA_SILVER}"
UC_GOLD = f"{CATALOG_NAME}.{SCHEMA_GOLD}"
UC_OPTIMIZATION = f"{CATALOG_NAME}.{SCHEMA_OPTIMIZATION}"
UC_QUALITY = f"{CATALOG_NAME}.{SCHEMA_QUALITY}"

print("Configured Unity Catalog namespaces:")
print(f"Bronze:       {UC_BRONZE}")
print(f"Silver:       {UC_SILVER}")
print(f"Gold:         {UC_GOLD}")
print(f"Optimization: {UC_OPTIMIZATION}")
print(f"Quality:      {UC_QUALITY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Define the expected source tables and their target table names.
# MAGIC
# MAGIC These names will be reused when reading the original file and writing Bronze Delta tables.

# COMMAND ----------

# DBTITLE 1,Define the expected source tables and their target table names
SOURCE_TABLES = {
    "order_list": "order_list",
    "freight_rates": "freight_rates",
    "plant_ports": "plant_ports",
    "products_per_plant": "products_per_plant",
    "wh_costs": "wh_costs",
    "wh_capacities": "wh_capacities",
    "vmi_customers": "vmi_customers"
}

BRONZE_TABLES = {
    table_name: f"{UC_BRONZE}.{table_name}"
    for table_name in SOURCE_TABLES.values()
}

SILVER_TABLES = {
    table_name: f"{UC_SILVER}.{table_name}"
    for table_name in SOURCE_TABLES.values()
}

SILVER_TABLES["valid_shipping_options"] = f"{UC_SILVER}.valid_shipping_options"

for table_key, table_name in BRONZE_TABLES.items():
    print(f"{table_key}: {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Define the target Delta locations.
# MAGIC
# MAGIC Define the target Delta locations for Bronze, Silver, Gold and Optimization tables.
# MAGIC
# MAGIC These locations will be used when writing external Delta tables to ADLS Gen2.

# COMMAND ----------

# DBTITLE 1,Define the target Delta locations
BRONZE_TABLE_PATHS = {
    table_name: f"{BRONZE_BASE_PATH}/{table_name}"
    for table_name in SOURCE_TABLES.values()
}

SILVER_TABLE_PATHS = {
    table_name: f"{SILVER_BASE_PATH}/{table_name}"
    for table_name in SOURCE_TABLES.values()
}

SILVER_TABLE_PATHS["valid_shipping_options"] = f"{SILVER_BASE_PATH}/valid_shipping_options"

GOLD_TABLES = {
    "dim_date": f"{UC_GOLD}.dim_date",
    "dim_product": f"{UC_GOLD}.dim_product",
    "dim_customer": f"{UC_GOLD}.dim_customer",
    "dim_plant": f"{UC_GOLD}.dim_plant",
    "dim_port": f"{UC_GOLD}.dim_port",
    "dim_carrier": f"{UC_GOLD}.dim_carrier",
    "dim_transport_mode": f"{UC_GOLD}.dim_transport_mode",
    "dim_service_level": f"{UC_GOLD}.dim_service_level",
    "fact_order_shipment": f"{UC_GOLD}.fact_order_shipment",
    "fact_freight_cost_option": f"{UC_GOLD}.fact_freight_cost_option",
    "fact_warehouse_capacity_usage": f"{UC_GOLD}.fact_warehouse_capacity_usage"
}

GOLD_TABLE_PATHS = {
    "dim_date": f"{GOLD_BASE_PATH}/dim_date",
    "dim_product": f"{GOLD_BASE_PATH}/dim_product",
    "dim_customer": f"{GOLD_BASE_PATH}/dim_customer",
    "dim_plant": f"{GOLD_BASE_PATH}/dim_plant",
    "dim_port": f"{GOLD_BASE_PATH}/dim_port",
    "dim_carrier": f"{GOLD_BASE_PATH}/dim_carrier",
    "dim_transport_mode": f"{GOLD_BASE_PATH}/dim_transport_mode",
    "dim_service_level": f"{GOLD_BASE_PATH}/dim_service_level",
    "fact_order_shipment": f"{GOLD_BASE_PATH}/fact_order_shipment",
    "fact_freight_cost_option": f"{GOLD_BASE_PATH}/fact_freight_cost_option",
    "fact_warehouse_capacity_usage": f"{GOLD_BASE_PATH}/fact_warehouse_capacity_usage"
}

OPTIMIZATION_TABLES = {
    "fact_optimized_order_assignment": f"{UC_OPTIMIZATION}.fact_optimized_order_assignment",
    "fact_cost_comparison": f"{UC_OPTIMIZATION}.fact_cost_comparison",
    "fact_optimized_capacity_usage": f"{UC_OPTIMIZATION}.fact_optimized_capacity_usage",
    "fact_unassigned_order_diagnostic": f"{UC_OPTIMIZATION}.fact_unassigned_order_diagnostic"
}

OPTIMIZATION_TABLE_PATHS = {
    "fact_optimized_order_assignment": f"{OPTIMIZATION_BASE_PATH}/fact_optimized_order_assignment",
    "fact_cost_comparison": f"{OPTIMIZATION_BASE_PATH}/fact_cost_comparison",
    "fact_optimized_capacity_usage": f"{OPTIMIZATION_BASE_PATH}/fact_optimized_capacity_usage",
    "fact_unassigned_order_diagnostic": f"{OPTIMIZATION_BASE_PATH}/fact_unassigned_order_diagnostic"
}

print("Bronze table paths configured:")
for table_name, path in BRONZE_TABLE_PATHS.items():
    print(f"{table_name}: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Define the Unity Catalog external volume path for Landing files.
# MAGIC
# MAGIC The volume is used to access the raw source file.

# COMMAND ----------

# DBTITLE 1,Define the Unity Catalog external volume path for Landing files.
LANDING_VOLUME_PATH = "/Volumes/az_supplychain/bronze/landing_files"
LANDING_VOLUME_SOURCE_PATH = f"{LANDING_VOLUME_PATH}/source={SOURCE_SYSTEM}"

print(f"Landing volume path:        {LANDING_VOLUME_PATH}")
print(f"Landing volume source path: {LANDING_VOLUME_SOURCE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Validate access to the Landing volume.
# MAGIC
# MAGIC This confirms that Unity Catalog, the external volume and the ADLS Gen2 permissions are working correctly.

# COMMAND ----------

# DBTITLE 1,Validate access to the Landing volume
landing_files = dbutils.fs.ls(LANDING_VOLUME_SOURCE_PATH)

display(landing_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Unity Catalog schemas validation.
# MAGIC
# MAGIC Validate that the required Unity Catalog schemas exist.
# MAGIC
# MAGIC This check prevents future notebooks from failing because of missing catalog or schema objects.

# COMMAND ----------

# DBTITLE 1,Unity Catalog schemas validation
required_schemas = {
    SCHEMA_BRONZE,
    SCHEMA_SILVER,
    SCHEMA_GOLD,
    SCHEMA_OPTIMIZATION,
    SCHEMA_QUALITY
}

schemas_df = spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}")
existing_schemas = {row.databaseName for row in schemas_df.collect()}

missing_schemas = required_schemas - existing_schemas

if missing_schemas:
    raise ValueError(f"Missing required schemas in catalog {CATALOG_NAME}: {missing_schemas}")

display(schemas_df)
print("All required Unity Catalog schemas exist.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - Set the active Unity Catalog catalog and schema for the current session.
# MAGIC
# MAGIC Bronze is selected as the default schema because the next notebook will create Bronze Delta tables.

# COMMAND ----------

# DBTITLE 1,Set the active Unity Catalog
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_BRONZE}")

current_catalog = spark.sql("SELECT current_catalog() AS current_catalog").collect()[0]["current_catalog"]
current_schema = spark.sql("SELECT current_schema() AS current_schema").collect()[0]["current_schema"]

print(f"Current catalog: {current_catalog}")
print(f"Current schema:  {current_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC