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
        .appName("prepare_and_save_data")
        .config(
            "spark.jars",
            Path(dirname(abspath(__file__)), "postgresql-42.2.11.jar"),
        )
        .getOrCreate()
    )

    data = spark_read_csv(
        spark, Path(dirname(abspath(__file__)), "resulting_data", "NY.csv"),
    )
    insert_to_db(data.drop("borough_from_raw_data"))
    print(f"{datetime.now()} - end program")
