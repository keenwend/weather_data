-----------------------------------------------------------------------------------------------------------------------
                                                    Работа с базой данных
-----------------------------------------------------------------------------------------------------------------------
Поднимаем контейнер с базой:

	docker run -d --name weather_db -p 5432:5432 --env-file /db_init/.env postgres:14

Где в файле .env находится три переменные, которые необходимо задать:
	POSTGRES_USER
	POSTGRES_PASSWORD
	POSTGRES_DB

Создаём необходимые объекты в базе. ДДЛ скрипт находится в /db_init/ddl.sql



-----------------------------------------------------------------------------------------------------------------------
                                                      API Open Weather
-----------------------------------------------------------------------------------------------------------------------
Регистрируемся на сайте: https://openweathermap.org/

Получаем API ключ, ждём пока он активируется (может занять до нескольких часов)

Полезные ссылки по работе с данным API:
	https://github.com/csparpa/pyowm
	https://pythonhowtoprogram.com/how-to-use-weather-api-to-get-weather-data-in-python-3/
	https://pyowm.readthedocs.io/en/latest/v3/code-recipes.html

Выкачиваем справочник городов по ссылке: http://bulk.openweathermap.org/sample/city.list.json.gz
Поученный файл лежит в data/city.list.json



-----------------------------------------------------------------------------------------------------------------------
                                                      Python часть
-----------------------------------------------------------------------------------------------------------------------
Ограничиваем список городов, для которых будем запрашивать погоду несколькими крупными российскими городами; 
файл data/big_ru_cities.py содержит список айди данных городов.

Парсим json файл с городами и загружаем в базу справочник с выбраными нами городами городами: скрипт load_city_dim.py. 
Вначале пишем результат парсинга в csv файл (data/cities_dim.csv), после этот файл уже загружаем в базу в raw таблицу.

Далее для наших городов запрашиваем погоду с помощью метода weather_at_id: скрипт load_data_weather.py.
Принцип парсинга полученных данных и загрузки в базу такой же как и со справочником городов.

После прогрузки данных двух скриптов в результате имеем некую "витрину" с наблюдениями о погоде в определённых городах
в определённое время - "dm_weather_observations_v".

Так как не удалось подключиться с yandex datalens к базе развёрнутой локально в докере, то в скрипт
load_data_weather.py была добавлена функция которая выгружает витрину в csv файл для дальнейшей загрузки в yandex
datalens - data/dm_weather.csv



-----------------------------------------------------------------------------------------------------------------------
                                                  Yandex Datalens часть
-----------------------------------------------------------------------------------------------------------------------
Загружаем полученный csv файл витрины данных в даталенс. Используем коннектор для csv файла.

На основе данного подключения создаём датасет. Добавляем поле "ГЕОТОЧКА" на основе координат городов для
дальнейшего использования в визуализации "Карта".Получаем один датасет, на основе которого можем строить дашборд.
