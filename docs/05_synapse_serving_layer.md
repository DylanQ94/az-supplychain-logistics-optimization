# Synapse Serverless SQL Serving Layer

## Purpose

Azure Synapse Analytics Serverless SQL exposes the final Databricks Delta tables through SQL views and materialized outputs.

Synapse acts as the serving layer between the data lake and future Power BI reporting.

## Confirmed Resources

| Resource | Name |
|---|---|
| Synapse Workspace | `syn-supplychain-z` |
| Database | `supply_chain_analytics_db` |
| Credential | `SynapseWorkspaceManagedIdentity` |
| External Data Source | `ds_supply_chain` |
| File Format | `ParquetFileFormat` |

External data source location:

```text
abfss://supply-chain@sasupplychainz.dfs.core.windows.net
```

## Final Schemas

```text
gold
optimization
materialized
serving
```

The `external_data` schema is not part of the final architecture and should be removed if it exists.

## Delta Access

Synapse reads Delta folders directly from ADLS Gen2 using `OPENROWSET` with `FORMAT = 'DELTA'`.

Synapse does not read from Unity Catalog directly.

## CETAS Outputs

CETAS was used to create selected materialized Parquet summaries in:

```text
synapse_materialized/supply_chain_logistics/
```

Final materialized tables:

```text
materialized.historical_vs_optimized_summary
materialized.customer_savings_summary
materialized.carrier_cost_summary
materialized.warehouse_capacity_summary
materialized.route_cost_summary
```

## Serving Role

The role `supply_chain_serving_reader` should be created to provide read-only access to the curated serving schema for future consumers such as Power BI.

## Final Output

The final Synapse layer provides SQL-ready datasets for future reporting without moving the entire data model into a dedicated SQL database.
