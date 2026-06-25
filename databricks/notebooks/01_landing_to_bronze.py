# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01_landing_to_bronze
# MAGIC
# MAGIC ## Objective
# MAGIC
# MAGIC This notebook reads the original Supply Chain Logistics Excel file from the Landing volume and creates the Bronze Delta tables.
# MAGIC
# MAGIC The Bronze layer keeps the data close to the source. Only column name standardization and minimal technical metadata are added.
# MAGIC
# MAGIC ## Inputs
# MAGIC
# MAGIC - Configuration notebook: `00_config`
# MAGIC - Landing external volume: `/Volumes/az_supplychain/bronze/landing_files/source=figshare`
# MAGIC - Source Excel file: `Supply_chain_logistics_problem.xlsx`
# MAGIC - Unity Catalog catalog: `az_supplychain`
# MAGIC - Unity Catalog schema: `az_supplychain.bronze`
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC External Delta tables registered in Unity Catalog:
# MAGIC
# MAGIC - `az_supplychain.bronze.order_list`
# MAGIC - `az_supplychain.bronze.freight_rates`
# MAGIC - `az_supplychain.bronze.plant_ports`
# MAGIC - `az_supplychain.bronze.products_per_plant`
# MAGIC - `az_supplychain.bronze.wh_costs`
# MAGIC - `az_supplychain.bronze.wh_capacities`
# MAGIC - `az_supplychain.bronze.vmi_customers`
# MAGIC
# MAGIC ## Processing flow
# MAGIC
# MAGIC 1. Load shared project configuration from `00_config`.
# MAGIC 2. Define the source Excel file path explicitly.
# MAGIC 3. Define the known Excel sheets and target Bronze table names.
# MAGIC 4. Read each known Excel sheet.
# MAGIC 5. Rename columns to clear snake_case names.
# MAGIC 6. Add minimal technical metadata.
# MAGIC 7. Write each DataFrame as an external Delta table.
# MAGIC 8. Validate table counts in Unity Catalog.
# MAGIC
# MAGIC ## Important notes
# MAGIC
# MAGIC - This notebook does not perform business cleaning.
# MAGIC - Data type corrections are handled in Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0 - Install the Excel reader dependency
# MAGIC
# MAGIC This notebook reads an Excel workbook from Landing.
# MAGIC
# MAGIC Pandas requires the `openpyxl` library to read `.xlsx` files, so this dependency is installed at notebook level to make the ingestion reproducible.
# MAGIC
# MAGIC Note: It's needed once, it was commented for notation.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# MAGIC %pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Restart kernel
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Load project configuration
# MAGIC
# MAGIC This step imports the shared variables from `00_config`.
# MAGIC
# MAGIC The imported configuration includes the Unity Catalog names, ADLS paths, Landing volume path and Bronze table target locations.

# COMMAND ----------

# DBTITLE 1,Load project configuration
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Define the source Excel file path
# MAGIC
# MAGIC This step defines the exact Excel file that will be processed from Landing.
# MAGIC
# MAGIC The project uses one known source file, so the notebook does not include unnecessary logic to choose between multiple files.

# COMMAND ----------

# DBTITLE 1,Define the source Excel file path
SOURCE_EXCEL_FILE_NAME = "supply_chain_logistics_problem_dataset.xlsx"

SOURCE_EXCEL_FILE_PATH = f"{LANDING_VOLUME_SOURCE_PATH}/{SOURCE_EXCEL_FILE_NAME}"

print(f"Source Excel file path: {SOURCE_EXCEL_FILE_PATH}")

# COMMAND ----------

dbutils.fs.ls(LANDING_VOLUME_SOURCE_PATH)

# COMMAND ----------

dbutils.fs.ls(SOURCE_EXCEL_FILE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Import required libraries
# MAGIC
# MAGIC This step imports Pandas to read Excel sheets and PySpark functions to add the minimal Bronze metadata.
# MAGIC
# MAGIC Pandas is used only because the source file is an Excel workbook. The final storage format is Delta Lake.

# COMMAND ----------

# DBTITLE 1,Import required libraries
import pandas as pd

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 - Define known source sheets, target tables and column names
# MAGIC
# MAGIC This step defines the expected Excel sheets and their Bronze target tables.
# MAGIC
# MAGIC Column names are explicitly mapped to snake_case names. This keeps the notebook clear and avoids unnecessary dynamic column detection.

# COMMAND ----------

# DBTITLE 1,Define known source sheets, target tables and column names
bronze_table_config = {
    "order_list": {
        "sheet_name": "OrderList",
        "target_table": f"{UC_BRONZE}.order_list",
        "target_path": BRONZE_TABLE_PATHS["order_list"],
        "column_mapping": {
            "Order ID": "order_id",
            "Order Date": "order_date",
            "Origin Port": "orig_port",
            "Carrier": "carrier",
            "TPT": "tpt_day_count",
            "Service Level": "service_level",
            "Ship ahead day count": "ship_ahead_day_count",
            "Ship Late Day count": "ship_late_day_count",
            "Customer": "customer",
            "Product ID": "product_id",
            "Plant Code": "plant_code",
            "Destination Port": "dest_port",
            "Unit quantity": "unit_quant",
            "Weight": "weight"
        }
    },
    "freight_rates": {
        "sheet_name": "FreightRates",
        "target_table": f"{UC_BRONZE}.freight_rates",
        "target_path": BRONZE_TABLE_PATHS["freight_rates"],
        "column_mapping": {
            "Carrier": "carrier",
            "orig_port_cd": "orig_port",
            "dest_port_cd": "dest_port",
            "minm_wgh_qty": "min_weight_quant",
            "max_wgh_qty": "max_weight_quant",
            "svc_cd": "service_level",
            "minimum cost": "min_cost",
            "rate": "rate",
            "mode_dsc": "mode_dsc",
            "tpt_day_cnt": "tpt_day_count",
            "Carrier type": "carrier_type"
        }
    },
    "plant_ports": {
        "sheet_name": "PlantPorts",
        "target_table": f"{UC_BRONZE}.plant_ports",
        "target_path": BRONZE_TABLE_PATHS["plant_ports"],
        "column_mapping": {
            "Plant Code": "plant_code",
            "Port": "port"
        }
    },
    "products_per_plant": {
        "sheet_name": "ProductsPerPlant",
        "target_table": f"{UC_BRONZE}.products_per_plant",
        "target_path": BRONZE_TABLE_PATHS["products_per_plant"],
        "column_mapping": {
            "Plant Code": "plant_code",
            "Product ID": "product_id"
        }
    },
    "wh_costs": {
        "sheet_name": "WhCosts",
        "target_table": f"{UC_BRONZE}.wh_costs",
        "target_path": BRONZE_TABLE_PATHS["wh_costs"],
        "column_mapping": {
            "WH": "plant_code",
            "Cost/unit": "cost_per_unit"
        }
    },
    "wh_capacities": {
        "sheet_name": "WhCapacities",
        "target_table": f"{UC_BRONZE}.wh_capacities",
        "target_path": BRONZE_TABLE_PATHS["wh_capacities"],
        "column_mapping": {
            "Plant ID": "plant_code",
            "Daily Capacity ": "daily_capacity"
        }
    },
    "vmi_customers": {
        "sheet_name": "VmiCustomers",
        "target_table": f"{UC_BRONZE}.vmi_customers",
        "target_path": BRONZE_TABLE_PATHS["vmi_customers"],
        "column_mapping": {
            "Plant Code": "plant_code",
            "Customers": "customer"
        }
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Read the known Excel sheets into Bronze DataFrames
# MAGIC
# MAGIC This step reads each expected Excel sheet and applies only the predefined column renaming.
# MAGIC
# MAGIC No business transformations, type corrections or validations are applied here because those belong to the Silver layer.

# COMMAND ----------

# DBTITLE 1,Read the known Excel sheets into Bronze DataFrames
bronze_dataframes = {}

for table_name, config in bronze_table_config.items():
    pandas_df = pd.read_excel(
        SOURCE_EXCEL_FILE_PATH,
        sheet_name=config["sheet_name"]
    )

    expected_source_columns = list(config["column_mapping"].keys())
    actual_source_columns = list(pandas_df.columns)

    if actual_source_columns != expected_source_columns:
        raise ValueError(
            f"Column mismatch in sheet {config['sheet_name']}.\n"
            f"Expected columns: {expected_source_columns}\n"
            f"Actual columns:   {actual_source_columns}"
        )

    pandas_df = pandas_df.rename(columns=config["column_mapping"])
    pandas_df = pandas_df.where(pd.notnull(pandas_df), None)

    spark_df = spark.createDataFrame(pandas_df)

    spark_df = (
        spark_df
        .withColumn("source_file_path", F.lit(SOURCE_EXCEL_FILE_PATH))
        .withColumn("ingestion_timestamp", F.current_timestamp())
    )

    bronze_dataframes[table_name] = spark_df

    print(f"Loaded sheet: {config['sheet_name']} -> {table_name}")
    print(f"Rows: {spark_df.count()}")
    print(f"Columns: {len(spark_df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 - Preview the Bronze DataFrames
# MAGIC
# MAGIC This step displays a small sample of each Bronze DataFrame before writing to Delta.
# MAGIC
# MAGIC The purpose is to visually confirm that the sheets were read correctly and that the column names are standardized.

# COMMAND ----------

# DBTITLE 1,Preview the Bronze DataFrames
for table_name, spark_df in bronze_dataframes.items():
    print(f"Preview: {table_name}")
    display(spark_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 - Write Bronze DataFrames as external Delta tables
# MAGIC
# MAGIC This step writes each Bronze DataFrame to its physical ADLS location and registers it as a Unity Catalog table.
# MAGIC
# MAGIC The physical storage location is under the Bronze ADLS path.
# MAGIC
# MAGIC The logical table name is registered under `az_supplychain.bronze`.
# MAGIC
# MAGIC The notebook uses overwrite mode because the source file is static and Bronze is rebuilt completely during reprocessing.

# COMMAND ----------

# DBTITLE 1,Write Bronze DataFrames as external Delta tables
for table_name, spark_df in bronze_dataframes.items():
    target_table = bronze_table_config[table_name]["target_table"]
    target_path = bronze_table_config[table_name]["target_path"]

    (
        spark_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("path", target_path)
        .saveAsTable(target_table)
    )

    print(f"Written table: {target_table}")
    print(f"Physical path: {target_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 - Validate Bronze tables in Unity Catalog
# MAGIC
# MAGIC This step validates that the expected Bronze tables were created and returns their row counts.
# MAGIC
# MAGIC This is a useful quality check because it confirms that every source sheet was converted into a Delta table.

# COMMAND ----------

# DBTITLE 1,Validate Bronze tables in Unity Catalog
bronze_validation = []

for table_name, config in bronze_table_config.items():
    target_table = config["target_table"]
    target_path = config["target_path"]
    row_count = spark.table(target_table).count()

    bronze_validation.append({
        "bronze_table": table_name,
        "unity_catalog_table": target_table,
        "physical_path": target_path,
        "row_count": row_count
    })

bronze_validation_df = spark.createDataFrame(bronze_validation)

display(bronze_validation_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 - List tables in the Bronze schema
# MAGIC
# MAGIC This final step lists the tables available in `az_supplychain.bronze`.
# MAGIC
# MAGIC The expected result is one Bronze table for each source sheet in the Excel workbook.

# COMMAND ----------

# DBTITLE 1,List tables in the Bronze schema
display(spark.sql(f"SHOW TABLES IN {UC_BRONZE}"))