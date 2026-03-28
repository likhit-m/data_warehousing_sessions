-- 1) "Rank every facility by their TOTAL_REPORTED_DIRECT_EMISSIONS (highest to lowest) within each State. I want to see how these three functions handle facilities that might have the exact same emission value."

SELECT f.state, e.total_reported_direct_emissions,
    ROW_NUMBER() OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC) AS row_number, 
    RANK() OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC) AS rank,
    DENSE_RANK() OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC) AS dense_rank
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int;

-- 2) "For every facility in our database, I want to see how they compare to their 'Nearest Superior' in the same state. Specifically, show me the facility name, state, and their emissions. Then, create a column that pulls the emissions of the facility that is ranked exactly one position above them in that same state. Finally, calculate the 'Emissions Gap' between them."

SELECT *, (previous_rank_emission-total_reported_emissions) AS difference_in_emissions
FROM (
    SELECT f.state, f.facility_name,
        DENSE_RANK() OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC) AS dense_rank,
        e.total_reported_direct_emissions AS total_reported_emissions,
        LAG(e.total_reported_direct_emissions, 1, 0) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC) AS previous_rank_emission    
    FROM fact_emissions e
    JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int);

-- 3) "I want a report that shows every facility and its emissions. But I also need two more columns: The Total Emissions for that entire State (shown on every row for that state) and The Running Total of emissions within that state (ordered from highest emitter to lowest)."

SELECT DENSE_RANK() OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC) AS dense_rank, f.state, f.facility_name,
        e.total_reported_direct_emissions AS total_reported_emissions, 
        SUM(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_emissions_by_state,
        SUM(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_emissions
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int

-- 4) Write a query that calculates the Percentage Contribution of each facility to its state's total emissions
WITH emissions_by_state AS (
    SELECT f.state, f.facility_name,
            e.total_reported_direct_emissions AS total_reported_emissions, 
            SUM(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total_emissions_by_state
    FROM fact_emissions e
    JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int
    ORDER BY f.state ASC, total_reported_emissions DESC)

SELECT *, ROUND((total_reported_emissions/total_emissions_by_state) * 100,2) AS percentage_emissions_of_facility
FROM emissions_by_state

-- 5) Identify how each facility performs against the average of its specific industry sector.

WITH sector_avg_emissions AS (
SELECT i.industry_type_sectors, f.facility_name, e.total_reported_direct_emissions,
    AVG(e.total_reported_direct_emissions) OVER(PARTITION BY i.industry_type_sectors ORDER BY e.total_reported_direct_emissions DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS industry_average
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int
JOIN dim_industries i ON e.industry_type_pk = i.industry_type_id
ORDER BY i.INDUSTRY_TYPE_SECTORS ASC)

SELECT *, ROUND(ABS(industry_average- total_reported_direct_emissions), 2) AS difference
FROM sector_avg_emissions

-- 6) Identify which "Emissions Quartile" each facility falls into across the entire United States. Your output should include: facility_name and state, total_reported_direct_emissions and emissions_quartile: Use NTILE(4).

SELECT f.state, f.facility_name, e.total_reported_direct_emissions,
    NTILE(4) OVER(ORDER BY e.total_reported_direct_emissions DESC)
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int

-- 7) For every state, identify the highest emission value and calculate how far each facility is from that state leader.
WITH state_max_emissions AS (
SELECT f.state, f.facility_name, e.total_reported_direct_emissions,
    MAX(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS state_max
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int
ORDER BY 1 DESC)

SELECT *, ROUND((state_max- total_reported_direct_emissions), 2) AS difference
FROM state_max_emissions

-- 8) Compare each facility's emissions to both its State Average and the National Average in a single row.

SELECT f.state, f.facility_name, e.total_reported_direct_emissions,
    AVG(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS state_average,
    AVG(e.total_reported_direct_emissions) OVER(ORDER BY e.total_reported_direct_emissions DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS national_average
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int
ORDER BY 1 ASC

-- 9) Identify the #1 highest emitter in each industry sector and calculate how much every other facility in that same sector "trails" behind them.

WITH sector_wise_ranking As (
SELECT i.industry_type_sectors, f.facility_name, e.total_reported_direct_emissions,
    DENSE_RANK() OVER(PARTITION BY i.industry_type_sectors ORDER BY e.total_reported_direct_emissions DESC) AS sector_wise_ranking,
    MAX(e.total_reported_direct_emissions) OVER(PARTITION BY i.industry_type_sectors ORDER BY e.total_reported_direct_emissions DESC) AS max_per_sector
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int
JOIN dim_industries i ON e.industry_type_pk = i.industry_type_id
ORDER BY 1 ASC, 3 DESC)

SELECT *, ROUND((max_per_sector - total_reported_direct_emissions),2) As difference
FROM sector_wise_ranking;

-- 10) For every row, identify the Highest Emitter and the Lowest Emitter in that specific state, so we can see the range of emissions.

SELECT f.state, f.facility_name, e.total_reported_direct_emissions,
    MAX(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS state_max,
    MIN(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS state_min
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int
ORDER BY 1 DESC

-- 11) Identify facilities that are "Significant Emitters" within their state. A facility is considered a "Significant Emitter" if its emissions are higher than the average for that state.
WITH state_wise_avg_emissions AS (
SELECT f.state, f.facility_name, e.total_reported_direct_emissions,
    AVG(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS state_wise_avg
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int
ORDER BY 1 DESC),

emmission_significance AS (
SELECT *,
    CASE WHEN total_reported_direct_emissions > state_wise_avg THEN 'Significant Emitters'
    ELSE 'Other' END AS Emission_significance
FROM state_wise_avg_emissions)

SELECT state, facility_name FROM emmission_significance WHERE Emission_significance = 'Significant Emitters'

-- 12) Create a report that only shows the Top 3 highest-emitting facilities for every state.
SELECT * FROM (
    SELECT f.state, f.facility_name, e.total_reported_direct_emissions,
        DENSE_RANK() OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC) AS state_wise_rank
    FROM fact_emissions e
    JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int)
WHERE state_wise_rank IN (1, 2, 3)

-- 13) Find the Top 5 most carbon-intensive facilities per Industry Sector within Texas (TX) and California (CA).

SELECT i.industry_type_sectors, f.facility_name, e.total_reported_direct_emissions,
    DENSE_RANK() OVER(PARTITION BY i.industry_type_sectors ORDER BY e.total_reported_direct_emissions DESC) AS emittors_rank
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int
JOIN dim_industries i ON e.industry_type_pk = i.industry_type_id
WHERE f.state IN ('TX', 'CA')
QUALIFY emittors_rank <= 5;

-- 14) We want to find the facilities in each state that contribute to the top 80% of that state's total emissions. Identify the "Heavy Hitters" in each state.

SELECT f.state, f.facility_name, e.total_reported_direct_emissions,
    SUM(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_emissions,
    SUM(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS state_wise_emissions,
    ROUND(((SUM(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)/SUM(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))*100), 2) AS running_total_percentage
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int
QUALIFY running_total_percentage <= 80
ORDER BY 1, 4;

--15) We calculate the Standard Deviation $(\sigma)$ for each industry sector. Any facility whose emissions are more than 2 Standard Deviations above the average is flagged as a "Critical Outlier."

SELECT i.industry_type_sectors, f.facility_name, e.total_reported_direct_emissions,
    AVG(e.total_reported_direct_emissions) OVER(PARTITION BY i.industry_type_sectors) AS sector_avg,
    STDDEV(e.total_reported_direct_emissions) OVER(PARTITION BY i.industry_type_sectors) AS sector_std,
    CASE   
        WHEN e.total_reported_direct_emissions > (sector_avg + (2* sector_std)) THEN 'OUTLIER'
        ELSE 'NORMAL' END AS outlier_flag
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int
JOIN dim_industries i ON e.industry_type_pk = i.industry_type_id
QUALIFY outlier_flag = 'OUTLIER'
ORDER BY 1

/* 16) For every facility, I want to see: (i) Facility-to-Sector %: What % of its industry's emissions does this one facility account for and (ii) Sector-to-National %: What % of the entire country's emissions does this facility's industry account for?"

The Task: Write a query that returns:
1. facility_name and industry_type_sectors.
2. total_reported_direct_emissions.
3. facility_pct_of_sector: (facility_emissions / SUM(emissions) OVER(PARTITION BY sector)) * 100.
4. sector_pct_of_national: (SUM(emissions) OVER(PARTITION BY sector) / SUM(emissions) OVER()) * 100. */

WITH industry_country_emissions AS (
SELECT i.industry_type_sectors, f.facility_name, e.total_reported_direct_emissions,
    SUM(e.total_reported_direct_emissions) OVER(PARTITION BY i.industry_type_sectors ORDER BY e.total_reported_direct_emissions ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS industry_wise_emissions,
    SUM(e.total_reported_direct_emissions) OVER(ORDER BY e.total_reported_direct_emissions ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS country_emissions
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int
JOIN dim_industries i ON e.industry_type_pk = i.industry_type_id
ORDER BY 1)

SELECT *, ROUND(((total_reported_direct_emissions/industry_wise_emissions)*100), 2) AS facility_pct_of_sector,
    ROUND(((industry_wise_emissions/country_emissions)*100), 2) AS sector_pct_of_national
FROM industry_country_emissions
ORDER BY sector_pct_of_national DESC, facility_pct_of_sector DESC

-- 17) "For every state, I want to identify the exact facility that causes the cumulative emissions for that state to cross the 1,000,000 metric ton threshold (starting from the largest emitter downwards)." Write a query that identifies only one facility per state: the one that "breaks the bank.

SELECT * FROM (
    SELECT *,
        DENSE_RANK() OVER(PARTITION BY state ORDER BY cumulative_emissions) AS rank_of_emissions
    FROM (
        SELECT f.state, f.facility_name, e.total_reported_direct_emissions,
            SUM(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_emissions
        FROM fact_emissions e
        JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int
        QUALIFY cumulative_emissions >= 1000000
        ORDER BY 1, 2))
WHERE rank_of_emissions = 1

-- 18) Identify the "Concentration Ratio" for each state. Specifically, find how many facilities it takes to reach the top 50% of emissions in that state. Your output should include: state and facility_count_for_top_50: A count of the facilities that make up the first 50% of the state's total emissions, total_facilities_in_state: The total count of facilities in that state and concentration_index: (facility_count_for_top_50 / total_facilities_in_state).

WITH facilities_for_top_50 AS (
SELECT f.state, f.facility_name, e.total_reported_direct_emissions,
    SUM(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_emissions,
    ROUND(((SUM(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)/SUM(e.total_reported_direct_emissions) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))*100), 2) AS running_total_percentage,
    COUNT(f.facility_name) OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS number_of_facilities_in_state
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int)

SELECT ft.state, COUNT(ft.facility_name) AS facility_count_for_top_50, MAX(number_of_facilities_in_state) AS total_facilities_in_state,
    ROUND((COUNT(*) * 1.0)/MAX(number_of_facilities_in_state), 4) AS concentration_index
FROM facilities_for_top_50 ft
WHERE running_total_percentage <= 50
GROUP BY state
ORDER BY concentration_index DESC

-- 19) Identify facilities that are in the Top 5% (95th percentile) of emitters in their State, but are NOT in the Top 5% of their Industry Sector nationally.

SELECT f.facility_name, f.state, i.industry_type_sectors, e.total_reported_direct_emissions,
    PERCENT_RANK() OVER(PARTITION BY f.state ORDER BY e.total_reported_direct_emissions ASC) state_percentile,
    PERCENT_RANK() OVER(PARTITION BY i.industry_type_sectors ORDER BY e.total_reported_direct_emissions ASC) sector_percentile
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int
JOIN dim_industries i ON e.industry_type_pk = i.industry_type_id
QUALIFY state_percentile >= 0.95 AND sector_percentile < 0.95
ORDER BY 2 

-- 20) Identify the "Distance from Typical" for every facility. Specifically, calculate how much each facility's emissions deviate from the Median of its specific industry sector.

SELECT *, ABS(total_reported_direct_emissions - sector_median) AS distance_from_median
FROM(
SELECT f.facility_name, i.industry_type_sectors, e.total_reported_direct_emissions,
    MEDIAN(e.total_reported_direct_emissions) OVER(PARTITION BY industry_type_sectors) sector_median
FROM fact_emissions e
JOIN dim_facilities f ON e.facility_id_pk = f.facility_id_int
JOIN dim_industries i ON e.industry_type_pk = i.industry_type_id
ORDER BY 2);

