# Azure Data Factory Ingestion

## Purpose

Azure Data Factory was used to ingest the original Supply Chain Logistics dataset from Figshare into Azure Data Lake Storage Gen2.

ADF was intentionally limited to ingestion. It did not parse sheets, infer schema, clean data, or apply transformations.

## Confirmed Resources

| Resource | Name |
|---|---|
| Resource Group | `rg-supplychain-dev` |
| Storage Account | `sasupplychainz` |
| ADLS Container | `supply-chain` |
| Azure Data Factory | `adf-supplychainz-dev` |
| Pipeline | `pl_ingest_supply_chain_dataset` |
| Source File | `supply_chain_logistics_problem_dataset.xlsx` |

## Landing Path

```text
abfss://supply-chain@sasupplychainz.dfs.core.windows.net/landing/supply_chain_logistics/source=figshare/supply_chain_logistics_problem_dataset.xlsx
```

The `version=1` folder was removed from the project to keep the Landing path simple.

## Final Scope

ADF performs a binary copy of the original Excel file into the Landing layer.

All parsing, cleaning, standardization, modeling, and optimization logic is handled later in Databricks.

## Design Decisions

- ADF was used only for ingestion.
- No transformations were implemented in ADF.
- No daily trigger was created because the source dataset is static.
- No additional logging tables were created because ADF already provides execution monitoring.
