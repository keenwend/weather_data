from files import load_list_dicts_to_csv
from database import load_csv_to_db, query_exec, load_sql_to_csv
from weather import get_weather
from data.big_ru_cities import BIG_RU_CITIES_IDS
from data.config_constants import WEATHER_API_KEY, PATH_TO_WEATHER_OBS_CSV, TARGET_TABLE_WEATHER_OBS, PATH_TO_DM_CSV
from data.dml import LOAD_WEATHER_OBSERVATIONS, LOAD_DM_TO_CSV


def main():
    weather_list = get_weather(WEATHER_API_KEY, BIG_RU_CITIES_IDS)
    load_list_dicts_to_csv(PATH_TO_WEATHER_OBS_CSV, weather_list)
    load_csv_to_db(PATH_TO_WEATHER_OBS_CSV, TARGET_TABLE_WEATHER_OBS)
    query_exec(LOAD_WEATHER_OBSERVATIONS)
    load_sql_to_csv(LOAD_DM_TO_CSV, PATH_TO_DM_CSV)


if __name__ == "__main__":
    main()
