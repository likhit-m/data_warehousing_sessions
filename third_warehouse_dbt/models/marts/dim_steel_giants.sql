{{ config(schema='marts', materialized='table') }}

with aggregations AS (
select 
    parent_name, 
    parent_type, 
    sum(weighted_emissions_co2e) as total_co2e_impact_tonnes
from {{ ref('fct_steel_emissions_attribution') }}
WHERE parent_name IS NOT NULL
group by 1, 2),

ranking as (
    select
        *,
        dense_rank() over (
            partition by parent_type 
            order by total_co2e_impact_tonnes desc
        ) as category_rank,

        dense_rank() over (
            order by total_co2e_impact_tonnes desc
        ) as global_rank
    from aggregations
)

select * from ranking
ORDER BY parent_type ASC, category_rank ASC
