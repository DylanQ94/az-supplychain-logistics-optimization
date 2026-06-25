# Az-SupplyChain — Azure Supply Chain Logistics Optimization Platform

Az-SupplyChain is a data engineering project built on Azure to ingest, process, model, and expose a supply chain logistics dataset for analytical consumption.

The current scope of the project goes from ingestion to the Synapse Serverless SQL serving layer.

## Project Scope

Implemented:

- Azure Data Factory ingestion
- Azure Data Lake Storage Gen2 storage layers
- Azure Databricks processing with Delta Lake
- Unity Catalog organization
- Bronze, Silver, Gold, and Optimization layers
- Azure Synapse Analytics Serverless SQL serving layer
- SQL views and CETAS materialized outputs

## Dataset Source

This project uses the public **Supply Chain Logistics Problem Dataset**, published on Figshare by Brunel University London.

Dataset reference:

* **Title:** Supply Chain Logistics Problem Dataset
* **Authors:** Tatiana Kalganova, Ivars Dzalbs
* **Source:** [Brunel Figshare — Supply Chain Logistics Problem Dataset](https://brunel.figshare.com/articles/dataset/Supply_Chain_Logistics_Problem_Dataset/7558679)
* **DOI:** [10.17633/rd.brunel.7558679](https://doi.org/10.17633/rd.brunel.7558679)

The dataset was used only for educational and portfolio purposes to build an Azure-based data engineering pipeline for supply chain logistics analysis and optimization.

## Architecture

```text
Figshare Dataset
    ↓
Azure Data Factory
    ↓
ADLS Gen2 - Landing
    ↓
Azure Databricks + Unity Catalog
    ↓
Bronze / Silver / Gold / Optimization Delta Tables
    ↓
Azure Synapse Analytics Serverless SQL
    ↓
Serving Views / CETAS Outputs
```

## Main Technologies

- Azure Data Factory
- Azure Data Lake Storage Gen2
- Azure Databricks
- Unity Catalog
- Delta Lake
- Azure Synapse Analytics Serverless SQL
- SQL
- PySpark

## Repository Structure

```text
.
├── README.md
├── docs/
│   ├── 01_project_overview.md
│   ├── 02_architecture.md
│   ├── 03_adf_ingestion.md
│   ├── 04_databricks_processing.md
│   ├── 05_synapse_serving_layer.md
│   ├── 06_data_model.md
│   ├── 07_results_and_conclusions.md
│   └── 08_pending_powerbi.md
├── adf/
├── notebooks/
├── synapse/
└── assets/
```

## Final State

The project currently exposes curated Gold and Optimization data through Synapse Serverless SQL. This layer is ready to be consumed by Power BI in a future phase.

## Notes

Unity Catalog and CETAS were included partly for learning and portfolio value. For this dataset size, they were not strictly required, but they helped practice a more realistic Azure data engineering architecture.
