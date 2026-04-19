{{ config(materialized='table', schema='marts') }}

with country_stats as (
    select
        country_code,
        year(start_date) as emission_year,
        count(distinct source_id) as asset_count,
        sum(weighted_emissions_co2e) as total_weighted_emissions,
        avg(weighted_emissions_co2e) as avg_emissions_per_asset
    from {{ ref('fct_steel_emissions_attribution') }}
    group by 1, 2
),

global_averages as (
    select
        emission_year,
        avg(total_weighted_emissions) as global_avg_country_emissions
    from country_stats
    group by 1
)

select
    c.*,
    g.global_avg_country_emissions,
    -- Calculate variance from global average
    (c.total_weighted_emissions - g.global_avg_country_emissions) as variance_from_global_avg
from country_stats c
join global_averages g on c.emission_year = g.emission_year
order by c.emission_year desc, c.total_weighted_emissions desc