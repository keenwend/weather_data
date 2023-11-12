LOAD_DIM_CITIES = '''
    insert into dim_cities(
      select
        city_id,
        city_name,
        state,
        country_code,
        lon,
        lat
      from ins_dim_cities_v
                          );
        '''


LOAD_WEATHER_OBSERVATIONS = '''
    insert into weather_observations(
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
        precipitation_probability
      from ins_weather_observations_v
                                     );
    '''


LOAD_DM_TO_CSV = "copy (select * from dm_weather_observations_v) to stdout with csv header delimiter ';'"
