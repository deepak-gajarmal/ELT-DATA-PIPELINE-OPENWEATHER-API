-- models/intermediate/int_daily_weather_aggregates.sql

with base_data as (
    select 
        name,
        to_date(dt) as dt,  -- Extract the date part of the timestamp
        temp,
        temp_min,
        temp_max,
        humidity,
        wind_speed
    from {{ ref('base_weather_info') }}  -- Reference the staging model
),

aggregated_weather as (
    select
        name,
        dt,
        avg(temp) as daily_avg_temperature,   -- Calculate average temperature
        max(temp_max) as daily_max_temperature,   -- Calculate max temperature for the day
        min(temp_min) as daily_min_temperature,   -- Calculate min temperature for the day
        avg(humidity) as daily_avg_humidity,      -- Calculate average humidity
        avg(wind_speed) as daily_avg_windspeed,   -- Calculate average wind speed
        count(*) as total_observations            -- Count the total number of observations
    from base_data
    group by name, dt  -- Group by city and date
)

select * from aggregated_weather
