from pyspark.sql import SparkSession
from data.work_with_document import spark_read_csv, insert_to_db

import logging
import psycopg2
import configparser
from pathlib import Path
from os.path import dirname, abspath

_LOGGER = logging.getLogger(__name__)

def connect_postgresql():
    config = configparser.ConfigParser()
    config.read(Path(dirname(abspath(__file__)), "config.ini"))
    con = psycopg2.connect(
        database=config["PostgreSQL"]["database"],
        user=config["PostgreSQL"]["user"],
        password=config["PostgreSQL"]["password"],
        host=config["PostgreSQL"]["host"],
        port=config["PostgreSQL"]["port"],
    )
    _LOGGER.debug("Database opened successfully")
    return con

def create_table_reconstruct_ny(connect):
    cur = connect.cursor()
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS reconstruct
       (PermitType varchar,
       Borough varchar,
       Corr double precision)'''
    )
    _LOGGER.debug("Table accidents created successfully")
    connect.commit()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    _LOGGER.debug("start program")
    con = connect_postgresql()
    create_table_reconstruct_ny(con)
    con.close()
    spark = (
        SparkSession.builder.master("local")
            .appName("reconstruction_in_NY")
            .config(
            "spark.jars",
            Path(dirname(abspath(__file__)), "postgresql-42.2.11.jar"),
        )
            .getOrCreate()
    )
    reconstruct = spark_read_csv(
        spark,
        Path(
            dirname(abspath(__file__)),
            "resulting_data",
            "Reconstruct_by borough.csv",
        ),
    )
    _LOGGER.info("start insert data about reconstruction to database")
    insert_to_db("reconstruct", reconstruct)
    _LOGGER.info("end insert data about reconstruction to database")
    _LOGGER.debug("end program")




