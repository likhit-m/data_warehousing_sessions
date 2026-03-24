# 🚀 Project 01: The EPA GHG Emissions Warehouse (2023)
## Status: ✅ Completed

Objective: Transform raw EPA Greenhouse Gas Reporting Program (GHGRP) data into a normalized Star Schema.

## 🏗️ Architecture Highlights:
### Bronze Layer (Raw Ingestion)
Created Snowflake Internal Stages with custom CSV file formats to handle multi-line headers and regulatory metadata. Automated ingestion pipeline using COPY INTO with error handling for malformed records.

### Silver Layer (Data Quality Engineering)
Implemented deduplication logic using ROW_NUMBER() OVER (PARTITION BY) to resolve duplicate FRS facility registrations
Type-casted VARCHAR regulatory codes to INTEGER for join optimization
Cleansed confidential data markers and standardized NULL handling across 10 emission gas columns
Reduced dataset from 6.2K to 6.1K clean records with full audit trail

### Gold Layer (Dimensional Modeling)
Built Star Schema with:
FACT_EMISSIONS: Granularity at facility × industry × gas type level
DIM_FACILITIES: 11 attributes including geospatial coordinates
DIM_INDUSTRIES: NAICS hierarchy with sector/subsector classification

## 💡 Engineering Challenge Solved:
Hit a classic Many-to-Many join explosion (6K → 51K rows) when linking facilities to industries. Root cause: Multiple NAICS codes per facility. Solution: Designed composite surrogate key (FACILITY_ID + NAICS_CODE) ensuring 1:1 cardinality and query correctness
