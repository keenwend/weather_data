import pyowm


def get_weather(api_key, city_ids_list):
    owm = pyowm.OWM(api_key).weather_manager()
    weather_list = []
    for id in city_ids_list:
        weather_list.append(owm.weather_at_id(id).to_dict())
    for d in weather_list:
        d.update(d["location"])
        del d["location"]
        del d["name"]
        del d["coordinates"]
        del d["country"]

    return weather_list
