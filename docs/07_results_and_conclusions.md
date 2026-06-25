# Results and Conclusions

## Main Results

This project produced a complete Azure data engineering flow from ingestion to SQL serving.

The final model supports analysis of:

- Historical shipment costs
- Optimized shipment assignments
- Warehouse capacity usage
- Customer-level savings
- Carrier-level costs
- Route-level cost behavior
- Unassigned order diagnostics

## Optimization Output

The optimization layer assigns orders using a greedy capacity-aware approach.

This approach is useful for learning and analytical comparison, but it is not a full mathematical optimization model.

## Technical Conclusions

- ADF was enough for ingestion because the source was a static file.
- Databricks was the right place for parsing, cleaning, modeling, and optimization.
- Unity Catalog improved organization and governance, even if it was not strictly required for this small dataset.
- Synapse Serverless SQL worked well as a serving layer over Delta and Parquet data in ADLS.
- CETAS was useful to practice materialized SQL outputs, although not all datasets required materialization.

## Known Limitations

- The source dataset is static.
- The optimization logic is heuristic, not globally optimal.
- Power BI has not been implemented yet.
- Some materialized summaries have limited analytical value because of the dataset structure.
