from pathlib import Path
from os.path import dirname, abspath
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import when

from data.work_with_document import spark_read_csv


print(f"{datetime.now()} - start primary processing")
spark = SparkSession.builder.master("local").getOrCreate()
cwd = dirname(abspath(__file__))
accidents = spark_read_csv(
    spark, Path(cwd, "data", "resulting_data", "test.csv")
)
weather = spark_read_csv(
    spark, Path(cwd, "data", "resulting_data", "weather.csv")
)

print(datetime.now(), "----", accidents.count())

weather = weather.withColumn(
    "station",
    when(weather.station == "KJFK", "BROOKLYN")
    .when(weather.station == "KEWR", "STATEN ISLAND")
    .when(weather.station == "KLGA", "BRONX, MANHATTAN")
    .when(weather.station == "KISP", "QUEENS")
    .otherwise("null"),
)

accidents = accidents.select(
    'borough', accidents.crash_datetime.cast('timestamp')
)
weather = weather.select(
    "station", "weather", weather.date_time.cast('timestamp')
)
accidents.createOrReplaceTempView("accidents")
weather.createOrReplaceTempView("weather")

merdge = spark.sql(
    """
    SELECT
    a.crash_datetime,
    a.borough,
    w.weather
    FROM accidents AS a
    INNER JOIN weather AS w
    ON a.borough == w.station
    WHERE (a.crash_datetime-w.date_time) == 30
    OR (w.date_time-a.crash_datetime) < -30
    """
)

final = merdge.collect()
print(datetime.now(), "----", "end")

final.show()
print(final.filter(final.weather.isNull()).count())
final.groupBy("weather").count().show()
