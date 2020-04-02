from pathlib import Path
from os.path import dirname, abspath
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import lag, col, when, unix_timestamp, sum
from pyspark.sql.window import Window

from data.work_with_document import spark_read_csv, insert_to_db
from create_table import connect_postgresql


_LOGGER = logging.getLogger(__name__)


def count_duration(spark, weather):
    weather_duration = spark.createDataFrame(
        [],
        schema=StructType(
            [
                StructField("weather", StringType(), True),
                StructField("duration, min", IntegerType(), True),
            ]
        ),
    )
    w = Window().orderBy(col("datetime"))
    for row in weather.groupBy("borough").count().collect():
        tmp_weather = weather.filter(weather.borough == row.borough)
        tmp_weather = tmp_weather.select(
            "*",
            lag("datetime", 1).over(w).alias("previous_datetime"),
            lag("datetime", -1).over(w).alias("next_datetime"),
        )
        tmp_weather = (
            tmp_weather.withColumn(
                "previous_datetime",
                when(
                    col("previous_datetime").isNull(), col("datetime")
                ).otherwise(col("previous_datetime")),
            )
            .withColumn(
                "next_datetime",
                when(
                    col("next_datetime").isNull(), col("datetime")
                ).otherwise(col("next_datetime")),
            )
            .withColumn(
                "duration, min",
                (
                    unix_timestamp(col("next_datetime"))
                    - unix_timestamp(col("previous_datetime"))
                )
                / 60,
            )
        )
        weather_duration = weather_duration.unionAll(
            tmp_weather.select("weather", "duration, min")
        )
    return weather_duration.groupBy("weather").agg(
        sum("duration, min").alias("duration_min")
    )


def create_table_weather_duration_ny():
    connect = connect_postgresql()
    cur = connect.cursor()
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS weather_duration_ny
        (id SERIAL PRIMARY KEY,
        weather varchar NOT NULL,
        duration_min float NOT NULL);'''
    )
    _LOGGER.info("Table about weather duration created successfully")
    connect.commit()
    con.close()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    _LOGGER.info("start program")

    cwd = dirname(abspath(__file__))
    spark = (
        SparkSession.builder.master("local")
        .appName("weather_duration")
        .config(
            "spark.jars",
            Path(dirname(abspath(__file__)), "postgresql-42.2.11.jar"),
        )
        .getOrCreate()
    )

    weather = spark_read_csv(
        spark, Path(cwd, "resulting_data", "weather.csv")
    )

    _LOGGER.info("start count weather duration")
    weather_duration = count_duration(spark, weather).repartition(4)
    _LOGGER.info("end count weather duration")

    con = connect_postgresql()
    create_table_weather_duration_ny()
    con.close()

    _LOGGER.info("start insert data about weather duration to database")
    insert_to_db("weather_duration_ny", weather_duration)
    _LOGGER.info("end insert data about weather duration to database")

    _LOGGER.info("end program")
