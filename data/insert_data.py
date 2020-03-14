from pathlib import Path
from os.path import dirname, abspath
from pyspark import SparkContext, SparkFiles
from pyspark.sql import *
from create_table import connect_postgresql
from prepare_data_pyspark import clear
from time import time, localtime


def insert_into_cities(connect, values):
    cur = connect.cursor()
    cur.execute(f"SELECT city FROM cities WHERE city = '{values}'")
    if cur.fetchall() == []:
        cur.execute(
            f"INSERT INTO cities (city) "
            f"VALUES ('{values}');"
        )
        print("Records about cities were inserted successfully")
    connect.commit()

def insert_into_boroughs(connect, values):
    cur = connect.cursor()
    cur.execute("SELECT id FROM cities WHERE city = 'New York' LIMIT 1")
    city = cur.fetchall()[0][0]
    for record in values.collect():
        cur.execute(
            f"INSERT INTO boroughs (city, borough, geom)"
            f"VALUES ({city}, '{record.BoroName}', '{record.the_geom}');"
        )
    connect.commit()
    print("Records about boroughs were inserted successfully")

def insert_into_accidents(connect, values):
    cur = connect.cursor()
    cur.execute("SELECT id FROM cities WHERE city = 'New York'")
    city = cur.fetchall()[0][0]
    cur.execute(f"SELECT id, borough FROM boroughs")
    boroughs = cur.fetchall()

    print("start insert", localtime(time()))
    for borough in boroughs:
        print(f"start insert {borough[1].upper()}", localtime(time()))
        for row in values.filter(values["borough"] == f"{borough[1].upper()}").collect():
            cur.execute(f"INSERT INTO accidents "
                        f"(crash_datetime, city, borough, location, weather, total_injury, total_killed) "
                        f"VALUES ('{row.date_time}', {city}, {borough[0]}, "
                        f"'{row.location}', 'clear', {row.total_injured}, {row.total_killed});")
        connect.commit()
        print(f"Records about {borough[1]} accidents were inserted successfully", localtime(time()))

    connect.commit()
    print(f"Records about accidents were inserted successfully", localtime(time()))


if __name__ == '__main__':
    con = connect_postgresql()

    #clean table
    cur = con.cursor()
    cur.execute("DELETE FROM cities")
    cur.execute("DELETE FROM boroughs")
    cur.execute("DELETE FROM accidents")
    con.commit()
    print("del")

    insert_into_cities(con, 'New York')

    spark = SparkSession \
        .builder\
        .getOrCreate()

    boroughs = spark.read.csv(
        SparkFiles.get(Path(dirname(abspath(__file__)), 'nybb.csv')),
        header=True,
        inferSchema=False)

    insert_into_boroughs(con, boroughs)


    insert_into_accidents(con, clear())







