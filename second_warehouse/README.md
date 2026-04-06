# 🚚 Global Logistics & Fulfillment Data Warehouse (second_warehouse)
## 📌 Executive Summary
This project demonstrates the end-to-end design and implementation of a cloud data warehouse in Snowflake. Using a Medallion Architecture (Raw → Staging → Marts), I transformed 1,000+ "dirty" logistics records into a single, high-performance Fact Table designed for executive lead-time and profitability reporting.

## 🏢 Business Problem
In a global supply chain, data is often fragmented and inconsistent. This project addresses three primary real-world challenges:
    1. Data Integrity: Removing duplicate orders (IDs 1050–1060) that would otherwise inflate revenue reporting.
    2. Standardization: Reconciling inconsistent regional naming (e.g., "europe" vs. "EMEA").
    3. Visibility Gap: Calculating complex fulfillment metrics (Lead Times, Net Margin, and Delivery Status) that are not present in raw source systems.

## 🛠️ Tech Stack & Methodology
Warehouse: Snowflake
Environment: VS Code (Source of Truth)
Version Control: Git (Feature-branch workflow)
Architecture: Medallion (Modular SQL Design)
Logic: Common Table Expressions (CTEs), Window Functions (QUALIFY), and Conditional Logic.

## 🏗️ Data Pipeline Architecture
1. Raw Layer (Bronze)
RAW_ORDERS: 1,000+ records containing order timestamps, customer IDs, and regional sales data.
RAW_SHIPMENTS: Carrier information, shipping/delivery timestamps, and logistics costs.

2. Staging Layer (Silver)
STG_ORDERS_VIEW:
Implemented Immutability: No UPDATE statements on raw data; all cleaning is done via views.
Deduplication: Utilized QUALIFY ROW_NUMBER() to ensure one record per order_id.
Standardization: Mapped inconsistent regional strings to a global standard (EMEA).

STG_SHIPMENTS_VIEW:
Cast timestamps for temporal accuracy.
Flagged delivery status to handle NULL values for lost/pending cargo.

3. Marts Layer (Gold)
FCT_FULFILLMENT: A single, wide Fact Table containing all business dimensions.
Metrics: LEAD_TIME_SHIPPING, LEAD_TIME_DELIVERY, and NET_MARGIN_USD.
Categorization: Implemented a status engine to flag FAST_TRACK, STANDARD, and LOST_OR_DELAYED orders based on regulatory and performance thresholds.

## 📈 Key Insights & Results
Efficiency: Reduced "Join Hell" for end-users by consolidating four fragmented tables into one unified Fact Table.
Auditability: Maintained a clean Git history with detailed commit messages for every schema change.
Scalability: The modular CTE-based SQL is designed to be easily migrated to dbt (Data Build Tool) for automated testing and deployment.