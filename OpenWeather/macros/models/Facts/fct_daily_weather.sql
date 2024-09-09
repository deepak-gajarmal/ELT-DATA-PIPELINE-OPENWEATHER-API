with daily_weather as (
    select
        name,
        dt as date,  -- Use date from the intermediate model
        daily_avg_temperature,
        daily_max_temperature,
        daily_min_temperature,
        daily_avg_humidity,
        daily_avg_windspeed,
        total_observations  -- Already calculated in intermediate model
    from {{ ref('int_daily_weather_aggregates') }}  -- Referencing intermediate model
)

select * from daily_weather
