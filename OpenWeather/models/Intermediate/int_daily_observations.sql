

with daily_observations as (
    select
        name,
        to_date(dt) as dt,  -- Group by date without time
        count(*) as total_observations  -- Count the number of observations per city per day
    from {{ ref('base_weather_info') }}
    group by name, to_date(dt)
)

select * from daily_observations
