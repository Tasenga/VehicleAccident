from pathlib import Path
from os.path import dirname, abspath
from datetime import datetime

from geospark.register import GeoSparkRegistrator
from pyspark.sql import SparkSession

from work_with_document import write_csv, spark_read_csv


def primary_processing(raw_data):
    tmp_sdf = raw_data.select(
        "crash_datetime",
        "city",
        "location",
        "weather",
        "total_injury",
        "total_killed",
    )
    primary_processed_data = tmp_sdf.withColumnRenamed(
        "total_injury", 'total_injured'
    )
    return primary_processed_data


def add_neighborhoods(spark, accidents, neighborhoods):
    accidents.createOrReplaceTempView("accidents")
    accidents_geom = spark.sql(
        """SELECT crash_datetime,
         city, st_geomFromWKT(location) as location,
         weather, total_injured, total_killed
         FROM accidents"""
    )
    accidents_geom.createOrReplaceTempView("accidents")

    neighborhoods.createOrReplaceTempView("neighborhoods")
    neighborhoods_geom = spark.sql(
        """
        SELECT COMMUNITY, st_geomFromWKT(the_geom) as neighborhood
        FROM neighborhoods
        """
    )
    neighborhoods_geom.createOrReplaceTempView("neighborhoods")

    data_with_neighborhoods = spark.sql(
        """
        SELECT
        a.crash_datetime,
        a.city,
        n.COMMUNITY as neighborhood,
        st_AsText(a.location) as location,
        a.weather,
        a.total_injured, a.total_killed
        FROM accidents AS a
        LEFT OUTER JOIN neighborhoods AS n
        ON ST_Intersects(a.location, n.neighborhood)
        """
    )
    return data_with_neighborhoods


def prepare_data_about_chicago(spark):
    print(f"{datetime.now()} - start primary processing")
    raw_data = spark_read_csv(
        spark,
        Path(
            dirname(abspath(__file__)), "data_source", "chicago_accidents.csv"
        ),
    )
    primary_processed_data = primary_processing(raw_data)
    print(f"{datetime.now()} - end primary processing")

    print(f"{datetime.now()} - start adding neighborhoods")
    neighborhoods = spark_read_csv(
        spark,
        Path(
            dirname(abspath(__file__)),
            "data_source",
            "chicago_neighborhoods.csv",
        ),
    )
    data_with_neighborhoods = add_neighborhoods(
        spark, primary_processed_data, neighborhoods
    )
    print(f"{datetime.now()} - end adding neighborhoods")

    print(f"{datetime.now()} - start create csv")
    write_csv(
        Path(cwd, "resulting_data", "Chicago.csv"),
        mode="w",
        values=[
            "crash_datetime",
            "city",
            "neighborhood",
            "location",
            "weather",
            "total_injured",
            "total_killed",
        ],
    )
    write_csv(
        Path(cwd, "resulting_data", "Chicago.csv"),
        mode="a",
        values=data_with_neighborhoods.collect(),
    )
    print(f"{datetime.now()} - end create csv")


if __name__ == '__main__':
    print(f"{datetime.now()} - start program")
    cwd = dirname(abspath(__file__))
    spark = SparkSession.builder.getOrCreate()
    GeoSparkRegistrator.registerAll(spark)
    prepare_data_about_chicago(spark)
    print(f"{datetime.now()} - end program")
