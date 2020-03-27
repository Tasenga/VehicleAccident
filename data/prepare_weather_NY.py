from pathlib import Path
from os.path import dirname, abspath

from pyspark.sql import SparkSession

from data.work_with_document import spark_read_csv


def prepare_weather_NY(spark, raw_weather):
    """
    from all type of weather in file, create nex set of weather types
    Cloudy, Fog, Freezing Rain, Heavy Rain, Heavy Snow,Mostly Cloudy,
    Partly Cloudy,Rain,Snow,T-Storm,Thunder,Windy,Wintry
    """

    valid_type_of_weather = spark.createDataFrame(
        [
            (['Heavy Snow / Windy', 'Heavy Snow with Thunder'], 'Heavy Snow'),
            (
                [
                    'Thunder / Windy',
                    'Thunder and Small Hail',
                    'Thunder in the Vicinity',
                ],
                'Thunder',
            ),
            (
                [
                    'Blowing Snow',
                    'Blowing Snow / Windy',
                    'Light Snow',
                    'Light Snow / Windy',
                    'Light Snow and Sleet',
                    'Light Snow and Sleet / Windy',
                    'Light Snow with Thunder',
                    'Snow / Windy',
                    'Snow and Sleet',
                    'Snow and Sleet / Windy',
                    'Snow and Thunder',
                    'Sleet',
                ],
                'Snow',
            ),
            (
                [
                    'Light Drizzle',
                    'Light Drizzle / Windy',
                    'Light Freezing Drizzle',
                    'Light Freezing Rain',
                    'Light Sleet',
                    'Light Sleet / Windy',
                    'Sleet / Windy',
                    'Small Hail',
                ],
                'Freezing Rain',
            ),
            (
                [
                    'Light Rain',
                    'Light Rain / Windy',
                    'Light Rain with Thunder',
                    'Rain / Windy',
                    'Rain and Sleet',
                ],
                'Rain',
            ),
            (
                [
                    'Drizzle and Fog',
                    'Drizzle and Fog / Windy',
                    'Haze',
                    'Haze / Windy',
                    'Fog / Windy',
                    'Mist',
                    'Patches of Fog',
                    'Patches of Fog / Windy',
                    'Shallow Fog',
                    'Smoke',
                ],
                'Fog',
            ),
            (['Partly Cloudy / Windy'], 'Partly Cloudy'),
            (['Mostly Cloudy / Windy'], 'Mostly Cloudy'),
            (['Cloudy / Windy'], 'Cloudy'),
            (
                ['Heavy T-Storm', 'Heavy T-Storm / Windy', 'T-Storm / Windy'],
                'T-Storm',
            ),
            (['Drizzle / Windy', 'Fair / Windy', 'Squalls / Windy'], 'Windy'),
            (['Unknown Precipitation'], 'Fair'),
            (['Wintry Mix / Windy', 'Wintry Mix', 'Mix'], 'Wintry'),
        ],
        ['variants', 'valid_type'],
    )

    valid_weather_data = (
        raw_weather.join(
            valid_type_of_weather,
            valid_type_of_weather.variants.cast("string").contains(
                raw_weather.weather
            ),
        )
        .select("date_time", "station", "valid_type")
        .withColumnRenamed("valid_type", "weather")
    )

    return valid_weather_data


if __name__ == '__main__':
    cwd = dirname(abspath(__file__))
    spark = SparkSession.builder.getOrCreate()
    weather = spark_read_csv(spark, Path(cwd, "resulting_data", 'weather.csv'))
    valid_weather_data = prepare_weather_NY(spark, weather)

    valid_weather_data.show()
