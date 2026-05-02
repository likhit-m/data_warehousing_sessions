{{ config(schema = 'marts', materialized = 'table')}}

WITH joining AS (
    SELECT 
        f.waste_asset_key,
        f.reporting_year,
        f.reporting_month,
        f.emissions_MT_CO2e,
        f.waste_quantity_MT,
        f.intensity_ration_CO2e,
        d.source_id,
        d.source_name,
        d.country,
        d.waste_type
    FROM {{ ref('fct_waste_assets') }} f
    INNER JOIN {{ ref('dim_waste_sites') }} d
        ON f.waste_asset_key = d.waste_asset_key
),

benchmarking AS (
    SELECT 
        *,
        AVG(intensity_ration_CO2e) OVER(PARTITION BY country, waste_type) AS country_sector_avg_intensity
    FROM joining
)

SELECT 
    *,
    ((intensity_ration_CO2e - country_sector_avg_intensity) / NULLIF(country_sector_avg_intensity, 0)) AS intensity_variance,
    
    DENSE_RANK() OVER(
        PARTITION BY country, waste_type 
        ORDER BY emissions_MT_CO2e DESC
    ) AS sector_rank_in_country
FROM benchmarking