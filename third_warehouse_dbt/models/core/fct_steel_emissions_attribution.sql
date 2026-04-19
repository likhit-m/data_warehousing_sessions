{{ config(schema = 'core')}}

with source_ownership_join AS (
SELECT 
    e.source_id,
    o.parent_id,
    e.source_name,
    o.parent_name,
    o.parent_type,
    e.country_code,
    e.start_date,
    e.end_date,
    e.emissions_quantity,
    e.uom,
    COALESCE(o.ownership_percent, 100) AS effective_ownership_percentage
FROM
    {{ ref("stg_emissions_sources")}} e
JOIN
    {{ ref('stg_emissions_ownership') }} o
ON e.source_id = o.source_id)

SELECT *, (emissions_quantity * (effective_ownership_percentage / 100)) as weighted_emissions_co2e
FROM source_ownership_join