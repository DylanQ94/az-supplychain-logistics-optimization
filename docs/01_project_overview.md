# Project Overview

## Objective

The objective of Az-SupplyChain is to build an Azure-based data engineering pipeline for a supply chain logistics dataset.

The project ingests raw logistics data, transforms it into curated analytical layers, builds a simple optimization model, and exposes the final data through Synapse Serverless SQL.

## Business Problem

The project focuses on logistics and shipment optimization questions such as:

- Which orders can be assigned under warehouse capacity constraints?
- What are the available freight options for each order?
- How do historical and optimized costs compare?
- Which customers, carriers, routes, or warehouses have relevant cost or capacity patterns?

## Current Scope

The current scope ends at Azure Synapse Analytics Serverless SQL.

Implemented:

- Data ingestion
- Data lake storage
- Data processing
- Data modeling
- Optimization outputs
- SQL serving layer

Not implemented yet:

- Power BI semantic model
- Dashboards
- DAX measures
- Report publishing

## Final Deliverable

The final deliverable of this phase is a clean Synapse Serverless SQL serving layer over Gold and Optimization datasets, ready for future Power BI consumption.
