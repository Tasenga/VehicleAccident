from pathlib import Path
from os.path import dirname, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, expr

from data.work_with_document import spark_read_csv


def prepare_weather_NY(spark, raw_weather):
    """
    from all type of weather in file, create nex set of weather types
    Cloudy, Fog, Freezing Rain, Heavy Rain, Heavy Snow,Mostly Cloudy,
    Partly Cloudy,Rain,Snow,T-Storm,Thunder,Windy,Wintry
    """

    valid_type_of_weather = spark.createDataFrame(
        [
            (
                [
                    'Heavy Snow / Windy',
                    'Heavy Snow with Thunder',
                    'Heavy Snow',
                ],
                'Heavy Snow',
            ),
            (
                [
                    'Thunder / Windy',
                    'Thunder and Small Hail',
                    'Thunder in the Vicinity',
                    'Thunder',
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
                    'Snow',
                ],
                'Snow',
            ),
            (
                [
                    'Drizzle',
                    'Light Drizzle',
                    'Light Drizzle / Windy',
                    'Light Freezing Drizzle',
                    'Light Freezing Rain',
                    'Light Sleet',
                    'Light Sleet / Windy',
                    'Light Snow / Freezing Rain',
                    'Sleet / Windy',
                    'Small Hail',
                    'Freezing Rain',
                    'Snow / Freezing Rain',
                    'Rain / Freezing Rain',
                    'Rain / Freezing Rain / Windy',
                    'Rain and Snow / Windy',
                    'Rain and Snow',
                    'Rain and Sleet / Windy',
                    'Rain and Sleet',
                    'Small Hail / Windy',
                ],
                'Freezing Rain',
            ),
            (
                [
                    'Light Rain',
                    'Light Rain / Windy',
                    'Light Rain with Thunder',
                    'Rain / Windy',
                    'Rain',
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
                    'Fog',
                ],
                'Fog',
            ),
            (['Partly Cloudy / Windy', 'Partly Cloudy'], 'Partly Cloudy'),
            (['Mostly Cloudy / Windy', 'Mostly Cloudy'], 'Mostly Cloudy'),
            (['Cloudy / Windy', 'Cloudy'], 'Cloudy'),
            (
                [
                    'Heavy T-Storm',
                    'Heavy T-Storm / Windy',
                    'T-Storm / Windy',
                    'T-Storm',
                ],
                'T-Storm',
            ),
            (
                [
                    'Drizzle / Windy',
                    'Fair / Windy',
                    'Squalls / Windy',
                    'Windy',
                ],
                'Windy',
            ),
            (['Unknown Precipitation', 'Fair'], 'Fair'),
            (['Wintry Mix / Windy', 'Wintry Mix', 'Mix', 'Wintry'], 'Wintry'),
            (['Heavy Rain / Windy', 'Heavy Rain'], 'Heavy Rain'),
        ],
        ['variants', 'valid_type'],
    )

    valid_weather_data = (
        raw_weather.join(
            valid_type_of_weather,
            expr("array_contains(variants, weather)"),
            how="leftouter",
        )
        .select("date_time", "station", "weather", "valid_type")
        .withColumn(
            "valid_type",
            when(col("valid_type").isNull(), col("weather")).otherwise(
                col("valid_type")
            ),
        )
        .drop(col("weather"))
        .withColumnRenamed("valid_type", "weather")
        .withColumn(
            "weather",
            when(col("weather").isNull(), "Fair").otherwise(col("weather")),
        )
        .withColumn(
            "station",
            when(col("station") == "KJFK", "Brooklyn")
            .when(col("station") == "KEWR", "Staten Island")
            .when(col("station") == "KLGA", "Bronx, Manhattan")
            .when(col("station") == "KISP", "Queens"),
        )
    )
    return valid_weather_data


if __name__ == '__main__':
    cwd = dirname(abspath(__file__))
    spark = SparkSession.builder.getOrCreate()
    weather = spark_read_csv(
        spark, Path(cwd, "resulting_data", 'weather.csv')
    )
    print(weather.count())
    valid_weather_data = prepare_weather_NY(spark, weather)
    print(valid_weather_data.count())
    valid_weather_data.groupBy("weather").count().show()
