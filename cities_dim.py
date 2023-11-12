import json
from data.big_ru_cities import BIG_RU_CITIES_IDS


def get_big_ru_cities_list(path_to_file):
    with open(path_to_file) as city_list_json:
        city_list = json.load(city_list_json)

    custom_city_list = [i for i in city_list if i["id"] in BIG_RU_CITIES_IDS]

    return custom_city_list


def get_only_cities_names(cities_list):
    custom_cities_names = []
    for i in range(len(cities_list)):
        custom_cities_names.append(cities_list[i]["name"])

    return custom_cities_names


if __name__ == "__main__":
    big_ru_cities_list = get_big_ru_cities_list("data/city.list.json")

    cities_names = get_only_cities_names(big_ru_cities_list)
