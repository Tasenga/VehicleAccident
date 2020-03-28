from pathlib import Path
from os.path import dirname, abspath
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import abs, unix_timestamp, min, to_date

from data.work_with_document import spark_read_csv
from data.prepare_weather_NY import prepare_weather_NY


print(f"{datetime.now()} - start primary processing")
spark = SparkSession.builder.master("local").getOrCreate()
cwd = dirname(abspath(__file__))
accidents = spark_read_csv(
    spark, Path(cwd, "data", "resulting_data", "NY.csv")
)
accidents = accidents.filter(
    (accidents.crash_datetime > "2019-12-01")
    & (accidents.crash_datetime < "2020-01-01")
)
print(datetime.now(), "----", accidents.count())

weather = prepare_weather_NY(
    spark,
    spark_read_csv(spark, Path(cwd, "data", "resulting_data", "weather.csv")),
)

distances = accidents.join(
    weather,
    (weather.station.contains(accidents.borough))
    & (to_date(accidents.crash_datetime) == to_date(weather.date_time)),
    how='leftouter',
).withColumn(
    'distance',
    abs(
        unix_timestamp(accidents.crash_datetime)
        - unix_timestamp(weather.date_time)
    ),
)
print(distances.groupBy('tmp_id').count().count())
min_distances = distances.groupBy("tmp_id").agg(
    min("distance").alias('distance')
)
print(min_distances.count())
result = distances.join(
    min_distances,
    (min_distances.tmp_id == distances.tmp_id)
    & (min_distances.distance == distances.distance),
    how="leftouter",
).select(
    min_distances["tmp_id"].alias("tmp_id"),
    "crash_datetime",
    "city",
    "borough",
    min_distances["distance"].alias("distance"),
    'weather',
)

result = result.dropDuplicates(["tmp_id"])
result.show()
print(result.count())
print(datetime.now())
