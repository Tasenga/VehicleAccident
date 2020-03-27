from pathlib import Path
from os.path import dirname, abspath
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    when,
    abs,
    unix_timestamp,
    min,
    to_date,
    monotonically_increasing_id,
)

from data.work_with_document import spark_read_csv
from data.prepare_weather_NY import prepare_weather_NY


print(f"{datetime.now()} - start primary processing")
spark = SparkSession.builder.master("local").getOrCreate()
cwd = dirname(abspath(__file__))
accidents = spark_read_csv(
    spark, Path(cwd, "data", "resulting_data", "NY.csv")
)
accidents = accidents.filter(
    (accidents.crash_datetime > "2019-12-30")
    & (accidents.crash_datetime < "2020-01-01")
)
print(datetime.now(), "----", accidents.count())
accidents = accidents.withColumn("tmp_id", monotonically_increasing_id())


weather = spark_read_csv(
    spark, Path(cwd, "data", "resulting_data", "weather.csv")
)

weather = weather.withColumn(
    "station",
    when(weather.station == "KJFK", "BROOKLYN")
    .when(weather.station == "KEWR", "STATEN ISLAND")
    .when(weather.station == "KLGA", "BRONX, MANHATTAN")
    .when(weather.station == "KISP", "QUEENS")
    .otherwise("null"),
)
weather = prepare_weather_NY(spark, weather)
distances = accidents.join(
    weather,
    (weather.station.contains(accidents.borough))
    & (to_date(accidents.crash_datetime) == to_date(weather.date_time)),
).withColumn(
    'distance',
    abs(
        unix_timestamp(accidents.crash_datetime)
        - unix_timestamp(weather.date_time)
    ),
)
print(distances.groupBy("tmp_id").count().count())
min_distances = distances.groupBy(
    "tmp_id", "crash_datetime", "city", "borough", "neighborhood", "location"
).agg(min("distance").alias('distance'))
print(min_distances.count())
result = distances.join(
    min_distances,
    [
        "tmp_id",
        "crash_datetime",
        "city",
        "borough",
        "neighborhood",
        "location",
        "distance",
    ],
    how="inner",
).select("crash_datetime", "borough", "distance", "weather")
result.show(5)
print(result.count())
print(datetime.now())
