# Databricks Processing

## Purpose

Azure Databricks processes the raw Excel dataset from the Landing layer and builds the analytical Delta Lake layers used by Synapse.

## Unity Catalog

Catalog:

```text
az_supplychain
```

Final schemas:

```text
az_supplychain.bronze
az_supplychain.silver
az_supplychain.gold
az_supplychain.optimization
```

## Notebooks

| Notebook | Purpose |
|---|---|
| `00_config` | Defines project configuration |
| `01_landing_to_bronze` | Loads raw Excel sheets into Bronze Delta tables |
| `02_bronze_to_silver_orders` | Cleans and standardizes order data |
| `03_bronze_to_silver_network_constraints` | Cleans logistics constraint tables |
| `04_create_valid_shipping_options` | Creates valid order shipping options |
| `05_silver_to_gold_dimensions` | Builds Gold dimension tables |
| `06_silver_to_gold_facts` | Builds Gold fact tables |
| `07_optimization_model` | Creates optimized order assignments |
| `08_optimization_diagnostics` | Explains unassigned orders and optimization results |
| `09_quality_checks` | Validates consistency across layers |

## Main Decisions

- Thin dimensions were kept to preserve a clean star schema and support future Power BI relationships.
- `order_id` was kept as a degenerate dimension inside facts instead of creating a separate `dim_order`.
- Quality checks were executed in notebooks, not persisted as extra quality tables.
- The optimization is a greedy capacity-aware assignment, not a global mathematical optimization model.

## Final Output

Databricks produces the final Gold and Optimization Delta tables consumed by Synapse Serverless SQL.
