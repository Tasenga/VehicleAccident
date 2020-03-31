from pathlib import Path
from os.path import dirname, abspath
from datetime import datetime

from pyspark.sql import SparkSession

from work_with_document import insert_to_db, spark_read_csv


if __name__ == '__main__':
    print(f"{datetime.now()} - start program")
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

    accidents = spark_read_csv(
        spark,
        Path(
            dirname(abspath(__file__)),
            "resulting_data",
            "NY_with_weather.csv",
        ),
    )
    insert_to_db("accidents", accidents.drop("tmp_id"))

    population = spark_read_csv(
        spark,
        Path(
            dirname(abspath(__file__)),
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

    print(f"{datetime.now()} - end program")
