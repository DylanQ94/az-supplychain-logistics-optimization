# Data Model

## Bronze Tables

```text
bronze.order_list
bronze.freight_rates
bronze.plant_ports
bronze.products_per_plant
bronze.wh_costs
bronze.wh_capacities
bronze.vmi_customers
```

Bronze stores the source sheets with minimal modification.

## Silver Tables

```text
silver.order_list
silver.freight_rates
silver.plant_ports
silver.products_per_plant
silver.wh_costs
silver.wh_capacities
silver.vmi_customers
silver.valid_shipping_options
```

Silver stores cleaned and standardized data ready for modeling.

## Gold Dimensions

```text
gold.dim_date
gold.dim_product
gold.dim_customer
gold.dim_plant
gold.dim_port
gold.dim_carrier
gold.dim_transport_mode
gold.dim_service_level
```

## Gold Facts

```text
gold.fact_order_shipment
gold.fact_freight_cost_option
gold.fact_warehouse_capacity_usage
```

## Optimization Tables

```text
optimization.fact_optimized_order_assignment
optimization.fact_optimized_capacity_usage
optimization.fact_unassigned_order_diagnostic
optimization.fact_cost_comparison
```

## Modeling Notes

- `order_id` is stored as a string.
- `order_id` is treated as a degenerate dimension in fact tables.
- Thin dimensions are kept for star schema clarity and future Power BI usage.
