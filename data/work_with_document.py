import csv
from json import loads, dump
from os import environ
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path

from pyspark import SparkFiles


def spark_read_csv(sparksession, filepath):
    return sparksession.read.csv(
        SparkFiles.get(filepath), inferSchema='true', header=True
    )


def spark_read_json(sparksession, filepath):
    return sparksession.read.option("multiline", "true").json(
        SparkFiles.get(filepath)
    )


def read_json(filepath):
    with filepath.open() as file:
        return [loads(row) for row in file]


def read_csv(filepath):
    with filepath.open() as file:
        return tuple(row for row in file)


def write_csv(filepath, mode, values):
    with filepath.open(mode=mode, newline="") as csv_file:
        writer = csv.writer(csv_file, delimiter=",")
        for line in values:
            writer.writerow(line)
        writer.writerow("")


def write_json(filepath, mode, values):
    with filepath.open(mode=mode) as file:
        dump(values, file)
        file.write("\n")


def insert_to_db(table, values):
    environ[
        'PYSPARK_SUBMIT_ARGS'
    ] = f"--jars file:///{Path('postgresql-42.2.11.jar')} pyspark-shell"

    db_properties = {}
    config = ConfigParser()
    config.read(Path("config.ini"))
    db_prop = config["PostgreSQL"]
    db_url = f"jdbc:postgresql://{db_prop['host']}/{db_prop['database']}"
    db_properties['username'] = db_prop['user']
    db_properties['password'] = db_prop['password']
    db_properties['driver'] = "org.postgresql.Driver"
    print(f"{datetime.now()} - successful connect to db")
    values.write.jdbc(
        db_url,
        table,
        mode="append",
        properties={
            "user": db_prop['user'],
            "password": db_prop['password'],
            "driver": "org.postgresql.Driver",
        },
    )
    print(f"{datetime.now()} - records were inserted successfully")
