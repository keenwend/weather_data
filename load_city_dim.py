from cities_dim import get_big_ru_cities_list
from files import load_list_dicts_to_csv
from database import load_csv_to_db, query_exec
from data.config_constants import PATH_TO_CITY_JSON, PATH_TO_CITY_DIM_CSV, TARGET_TABLE_CITY
from data.dml import LOAD_DIM_CITIES


def main():
    big_ru_cities_list = get_big_ru_cities_list(PATH_TO_CITY_JSON)
    load_list_dicts_to_csv(PATH_TO_CITY_DIM_CSV, big_ru_cities_list)
    load_csv_to_db(PATH_TO_CITY_DIM_CSV, TARGET_TABLE_CITY)
    query_exec(LOAD_DIM_CITIES)


if __name__ == "__main__":
    main()
