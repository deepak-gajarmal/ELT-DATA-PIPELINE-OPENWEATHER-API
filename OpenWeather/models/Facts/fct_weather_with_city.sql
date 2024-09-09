
with daily_weather as (
    select *
    from {{ ref('fct_daily_weather') }}  -- Fact model referencing intermediate model
),

city_details as (
    select *
    from {{ ref('dim_city') }}  -- Dimension model for city details
),

weather_conditions as (
    select *
    from {{ ref('dim_weather_condition') }}  -- Dimension model for weather conditions
)

select
    dw.name,
    dw.date,
    dw.daily_avg_temperature,
    dw.daily_max_temperature,
    dw.daily_min_temperature,
    dw.daily_avg_humidity,
    dw.daily_avg_windspeed,
    dw.total_observations,
    cd.longitude,
    cd.latitude,
    wc.condition_name,
    wc.condition_description
from daily_weather dw
left join city_details cd
on dw.name = cd.name
left join weather_conditions wc
on dw.name = wc.name
