from pathlib import Path
from os.path import dirname, abspath
import logging

from geospark.register import GeoSparkRegistrator
from pyspark.sql import SparkSession

from work_with_document import write_csv, spark_read_csv, insert_to_db


_LOGGER = logging.getLogger(__name__)


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
    _LOGGER.info("start primary processing")
    raw_data = spark_read_csv(
        spark,
        Path(
            dirname(abspath(__file__)), "data_source", "chicago_accidents.csv"
        ),
    )
    _LOGGER.info("end primary processing")

    _LOGGER.info("start adding neighborhoods")
    neighborhoods = spark_read_csv(
        spark,
        Path(
            dirname(abspath(__file__)),
            "data_source",
            "chicago_neighborhoods.csv",
        ),
    )
    data_with_neighborhoods = add_neighborhoods(
        spark, raw_data, neighborhoods
    )
    _LOGGER.info("end adding neighborhoods")

    _LOGGER.info("start create csv")
    write_csv(
        Path(cwd, "resulting_data", "Chicago.csv"),
        mode="w",
        values=[
            [
                "crash_datetime",
                "city",
                "neighborhood",
                "location",
                "weather",
                "total_injured",
                "total_killed",
            ]
        ],
    )
    write_csv(
        Path(cwd, "resulting_data", "Chicago.csv"),
        mode="a",
        values=data_with_neighborhoods.collect(),
    )
    _LOGGER.info("end create csv")


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    _LOGGER.info("start program")

    cwd = dirname(abspath(__file__))
    spark = (
        SparkSession.builder.master("local")
        .appName("insert data to db")
        .config(
            "spark.jars",
            Path(dirname(abspath(__file__)), "postgresql-42.2.11.jar"),
        )
        .getOrCreate()
    )
    GeoSparkRegistrator.registerAll(spark)

    # prepare_data_about_chicago(spark)
    accidents = spark_read_csv(
        spark,
        Path(dirname(abspath(__file__)), "resulting_data", "Chicago.csv",),
    )
    insert_to_db("accidents", accidents)

    _LOGGER.info("end program")
