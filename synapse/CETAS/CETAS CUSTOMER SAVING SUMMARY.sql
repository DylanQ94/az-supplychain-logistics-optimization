--- CREATING CETAS CUSTOMER SAVING SUMMARY ---

IF EXISTS (
    SELECT 1
    FROM sys.external_tables t
    INNER JOIN sys.schemas s
        ON t.schema_id = s.schema_id
    WHERE s.name = 'materialized'
      AND t.name = 'customer_savings_summary'
)
BEGIN
    DROP EXTERNAL TABLE materialized.customer_savings_summary;
END;
GO

CREATE EXTERNAL TABLE materialized.customer_savings_summary
WITH (
    LOCATION = 'synapse_materialized/supply_chain_logistics/customer_savings_summary/',
    DATA_SOURCE = ds_supply_chain,
    FILE_FORMAT = ParquetFileFormat
)
AS
SELECT
    customer,
    COUNT_BIG(*) AS optimized_order_count,
    SUM(historical_total_cost) AS historical_total_cost,
    SUM(optimized_total_cost) AS optimized_total_cost,
    SUM(estimated_savings) AS estimated_savings,
    CASE
        WHEN SUM(historical_total_cost) = 0 THEN NULL
        ELSE SUM(estimated_savings) / SUM(historical_total_cost)
    END AS savings_percentage,
    AVG(savings_percentage) AS average_order_savings_percentage
FROM optimization.vw_fact_optimized_order_assignment
GROUP BY
    customer;
GO

--- CETAS VALIDATION ---
SELECT *
FROM materialized.customer_savings_summary;