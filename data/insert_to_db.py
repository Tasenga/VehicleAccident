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

    data = spark_read_csv(
        spark,
        Path(
            dirname(abspath(__file__)),
            "data_source",
            "New_York_City_Population_By_Neighborhood_Tabulation_Areas.csv",
        ),
    )
    data = (
        data.withColumnRenamed("Borough", "borough")
        .withColumnRenamed("NTA Name", "neighborhood")
        .withColumnRenamed("Population", "population")
    )
    insert_to_db(
        "population_NY", data.drop("Year", "FIPS County Code", "NTA Code")
    )
    print(f"{datetime.now()} - end program")
