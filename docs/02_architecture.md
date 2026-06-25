# Architecture

![Extended_architecture](../assets/screenshots/extended_architecture_diagram_project.png)

## High-Level Flow

```text
Figshare Dataset
    ↓
Azure Data Factory
    ↓
ADLS Gen2 - Landing
    ↓
Azure Databricks
    ↓
Delta Lake: Bronze / Silver / Gold / Optimization
    ↓
Azure Synapse Analytics Serverless SQL
    ↓
Serving Views and CETAS Outputs
```

## Component Roles

| Component | Role |
|---|---|
| Azure Data Factory | Ingests the source file into ADLS Gen2 |
| ADLS Gen2 | Stores raw, processed, curated, and materialized data |
| Azure Databricks | Cleans, transforms, models, and optimizes the data |
| Unity Catalog | Organizes tables, schemas, volumes, and external locations |
| Delta Lake | Stores the analytical tables used by Databricks and Synapse |
| Synapse Serverless SQL | Exposes curated data through SQL views and materialized outputs |

## Design Principle

The project follows a simple rule: only resources that add value to the final architecture should remain. Temporary, exploratory, or unused resources should be removed before publishing the project.

## Current Boundary

Power BI is outside the current scope and will be developed later using the Synapse serving layer.
