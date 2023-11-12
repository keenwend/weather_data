create table raw_dim_cities (
  city_id                int,
  city_name              varchar,
  state                  varchar,
  country_code           varchar,
  coordinates            varchar
  );


create table dim_cities (
  city_id                int,
  city_name              varchar,
  state                  varchar,
  country_code           varchar,
  lon                    real,
  lat                    real,
  processed_dttm         timestamp default current_timestamp
  );


create or replace view ins_dim_cities_v as
  select
    city_id,
    city_name,
    state,
    country_code,
    cast(replace(coordinates, '''', '"')::json->>'lon' as real) as lon,
    cast(replace(coordinates, '''', '"')::json->>'lat' as real) as lat
  from raw_dim_cities
  where not exists
    (select 1 from dim_cities where dim_cities.city_id = raw_dim_cities.city_id)
    or exists
    (select 1 from dim_cities where dim_cities.city_id = raw_dim_cities.city_id
                              and (dim_cities.city_name != raw_dim_cities.city_name
                                or dim_cities.state != raw_dim_cities.state
                                or dim_cities.country_code != raw_dim_cities.country_code
                                or dim_cities.lon != cast(replace(raw_dim_cities.coordinates, '''', '"')::json->>'lon' as real)
                                or dim_cities.lat != cast(replace(raw_dim_cities.coordinates, '''', '"')::json->>'lat' as real)
                                  )
    );


create table raw_weather_observations (
  reception_time_unix             int,
  weather                         varchar,
  city_id                         int
  );


create table weather_observations (
  reception_time                       timestamptz,
  city_id                              int,
  reference_time                       timestamptz,
  sunset_time                          timestamptz,
  sunrise_time                         timestamptz,
  clouds                               int,
  rain                                 varchar,
  snow                                 varchar,
  wind_speed                           real,
  wind_deg                             int,
  wind_gust                            real,
  humidity                             int,
  pressure_press                       int,
  pressure_sea_level                   varchar,
  temperature_temp_c                   real,
  temperature_temp_kf                  varchar,
  temperature_temp_max_c               real,
  temperature_temp_min_c               real,
  temperature_feels_like_c             real,
  status                               varchar,
  detailed_status                      varchar,
  weather_code                         int,
  weather_icon_name                    varchar,
  visibility_distance                  int,
  dewpoint                             varchar,
  humidex                              varchar,
  heat_index                           varchar,
  utc_offset                           int,
  uvi                                  varchar,
  precipitation_probability            varchar,
  processed_dttm                       timestamp default current_timestamp
  );


create or replace view ins_weather_observations_v as
  select
    to_timestamp(reception_time_unix) as reception_time,
    city_id as city_id,
    to_timestamp(cast(replace(replace(weather, '''', '"'), 'None', '""')::json->>'reference_time' as int)) as reference_time,
    to_timestamp(cast(replace(replace(weather, '''', '"'), 'None', '""')::json->>'sunset_time' as int)) as sunset_time,
    to_timestamp(cast(replace(replace(weather, '''', '"'), 'None', '""')::json->>'sunrise_time' as int)) as sunrise_time,
    cast(replace(replace(weather, '''', '"'), 'None', '""')::json->>'clouds' as int) as clouds,
    replace(replace(weather, '''', '"'), 'None', '""')::json->>'rain' as rain,
    replace(replace(weather, '''', '"'), 'None', '""')::json->>'snow' as snow,
    cast(replace(replace(weather, '''', '"'), 'None', '""')::json->'wind'->>'speed' as real) as wind_speed,
    cast(replace(replace(weather, '''', '"'), 'None', '""')::json->'wind'->>'deg' as int) as wind_deg,
    cast(replace(replace(weather, '''', '"'), 'None', '""')::json->'wind'->>'gust' as real) as wind_gust,
    cast(replace(replace(weather, '''', '"'), 'None', '""')::json->>'humidity' as int) as humidity,
    cast(replace(replace(weather, '''', '"'), 'None', '""')::json->'pressure'->>'press' as int) as pressure_press,
    replace(replace(weather, '''', '"'), 'None', '""')::json->'pressure'->>'sea_level' as pressure_sea_level,
    cast(replace(replace(weather, '''', '"'), 'None', '""')::json->'temperature'->>'temp' as real) - 273.15 as temperature_temp_c,
    replace(replace(weather, '''', '"'), 'None', '""')::json->'temperature'->>'temperature_temp_kf' as temperature_temp_kf,
    cast(replace(replace(weather, '''', '"'), 'None', '""')::json->'temperature'->>'temp_max' as real) - 273.15 as temperature_temp_max_c,
    cast(replace(replace(weather, '''', '"'), 'None', '""')::json->'temperature'->>'temp_min' as real) - 273.15 as temperature_temp_min_c,
    cast(replace(replace(weather, '''', '"'), 'None', '""')::json->'temperature'->>'feels_like' as real) - 273.15 as temperature_feels_like_c,
    replace(replace(weather, '''', '"'), 'None', '""')::json->>'status' as status,
    replace(replace(weather, '''', '"'), 'None', '""')::json->>'detailed_status' as detailed_status,
    cast(replace(replace(weather, '''', '"'), 'None', '""')::json->>'weather_code' as int) as weather_code,
    replace(replace(weather, '''', '"'), 'None', '""')::json->>'weather_icon_name' as weather_icon_name,
    cast(replace(replace(weather, '''', '"'), 'None', '""')::json->>'visibility_distance' as int) as visibility_distance,
    replace(replace(weather, '''', '"'), 'None', '""')::json->>'dewpoint' as dewpoint,
    replace(replace(weather, '''', '"'), 'None', '""')::json->>'humidex' as humidex,
    replace(replace(weather, '''', '"'), 'None', '""')::json->>'heat_index' as heat_index,
    cast(replace(replace(weather, '''', '"'), 'None', '""')::json->>'utc_offset' as int) as utc_offset,
    replace(replace(weather, '''', '"'), 'None', '""')::json->>'uvi' as uvi,
    replace(replace(weather, '''', '"'), 'None', '""')::json->>'precipitation_probability' as precipitation_probability
  from raw_weather_observations
  where not exists
   (select 1
    from weather_observations
    where weather_observations.city_id = raw_weather_observations.city_id
      and weather_observations.reference_time =
      to_timestamp(cast(replace(replace(raw_weather_observations.weather, '''', '"'), 'None', '""')::json->>'reference_time' as int))
   );



create or replace view dm_weather_observations_v as
with city as (
select
  city_id,
  city_name,
  state,
  country_code,
  lon,
  lat,
  row_number() over(partition by city_id order by processed_dttm desc) as rn
from dim_cities
),
weather as (
select
  reception_time,
  city_id,
  reference_time,
  sunset_time,
  sunrise_time,
  clouds,
  rain,
  snow,
  wind_speed,
  wind_deg,
  wind_gust,
  humidity,
  pressure_press,
  pressure_sea_level,
  temperature_temp_c,
  temperature_temp_kf,
  temperature_temp_max_c,
  temperature_temp_min_c,
  temperature_feels_like_c,
  status,
  detailed_status,
  weather_code,
  weather_icon_name,
  visibility_distance,
  dewpoint,
  humidex,
  heat_index,
  utc_offset,
  uvi,
  precipitation_probability,
  row_number() over(partition by city_id, reference_time order by processed_dttm desc) as rn
from weather_observations
)
select
  weather.reception_time,
  weather.city_id,
  weather.reference_time,
  weather.sunset_time,
  weather.sunrise_time,
  weather.clouds,
  weather.rain,
  weather.snow,
  weather.wind_speed,
  weather.wind_deg,
  weather.wind_gust,
  weather.humidity,
  weather.pressure_press,
  weather.pressure_sea_level,
  weather.temperature_temp_c,
  weather.temperature_temp_kf,
  weather.temperature_temp_max_c,
  weather.temperature_temp_min_c,
  weather.temperature_feels_like_c,
  weather.status,
  weather.detailed_status,
  weather.weather_code,
  weather.weather_icon_name,
  weather.visibility_distance,
  weather.dewpoint,
  weather.humidex,
  weather.heat_index,
  weather.utc_offset,
  weather.uvi,
  weather.precipitation_probability,
  city.city_name,
  city.state,
  city.country_code,
  city.lon,
  city.lat
from weather
left join city on weather.city_id = city.city_id and city.rn = 1
where weather.rn = 1;

