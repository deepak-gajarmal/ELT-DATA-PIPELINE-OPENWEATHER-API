with max_min_temp_timestamps as (
    select
        name,
        to_date(dt) as dt,
        max(temp_max) as max_temperature,  -- Maximum temperature
        max_by(timestamp, temp_max) as max_temp_timestamp,  -- Timestamp of the max temperature
        min(temp_min) as min_temperature,  -- Minimum temperature
        min_by(timestamp, temp_min) as min_temp_timestamp   -- Timestamp of the min temperature
    from base_weather_info
    group by name, to_date(dt)
)

select * from max_min_temp_timestamps