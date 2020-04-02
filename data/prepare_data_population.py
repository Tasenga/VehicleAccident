from pathlib import Path
from os.path import dirname, abspath
import logging

from pyspark.sql import SparkSession

from data.work_with_document import spark_read_csv, insert_to_db
from create_table import connect_postgresql


_LOGGER = logging.getLogger(__name__)


def create_table_population():
    connect = connect_postgresql()
    cur = connect.cursor()
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS population_NY
        (id SERIAL PRIMARY KEY,
        borough varchar NOT NULL,
        neighborhood varchar NOT NULL,
        population int NOT NULL);'''
    )
    _LOGGER.info("Table about population created successfully")
    connect.commit()
    connect.close()


def create_table_processed_data_about_population():
    connect = connect_postgresql()
    cur = connect.cursor()
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS proc_population_NY
        (id SERIAL PRIMARY KEY,
        neighborhood varchar,
        population float,
        accidents int);'''
    )
    _LOGGER.info("Table about population created successfully")
    connect.commit()
    connect.close()


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

    create_table_population()
    population = spark_read_csv(
        spark,
        Path(
            cwd,
            "data_source",
            "New_York_City_Population_By_Neighborhood_Tabulation_Areas.csv",
        ),
    )
    population = (
        population.withColumnRenamed("Borough", "borough")
        .withColumnRenamed("NTA Name", "neighborhood")
        .withColumnRenamed("Population", "population")
    )
    insert_to_db(
        "population_NY",
        population.drop("Year", "FIPS County Code", "NTA Code"),
    )

    create_table_processed_data_about_population()
    population2 = spark_read_csv(
        spark, Path(cwd, "data_source", "neighborhoods_population.csv",),
    )
    insert_to_db("proc_population_NY", population2.drop("id", "id1"))

    _LOGGER.info("end program")
