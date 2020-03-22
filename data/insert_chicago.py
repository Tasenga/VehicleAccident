import configparser
from pathlib import Path
from os.path import dirname, abspath
from pyspark import SparkContext, SparkFiles
from pyspark.sql import *
from create_table import connect_postgresql
from time import time, localtime
from pyspark.sql.functions import when, year


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
    cur.execute("SELECT id FROM cities WHERE city = 'Chicago' LIMIT 1")
    city = cur.fetchall()[0][0]
    for record in values.collect():
        cur.execute(
            f"INSERT INTO boroughs (city, borough, geom)"
            f"VALUES ({city}, '{record.COMMUNITY}', '{record.the_geom}');"
        )
    connect.commit()
    print("Records about boroughs were inserted successfully")

def insert_into_accidents(connect, values):
    cur = connect.cursor()
    cur.execute("SELECT id FROM cities WHERE city = 'Chicago'")
    city = cur.fetchall()[0][0]

    values = values.withColumn(
        "weather",
        when(values.weather == "null", "clear").\
        otherwise(values.weather))
    for y in range(2019, 2021):
        print(f"start insert accidents for {y}", localtime(time()))
        for row in values.filter(year("crash_datetime") == y).collect():
            cur.execute(f"INSERT INTO accidents "
                        f"(crash_datetime, city, borough, location, weather, total_injury, total_killed) "
                        f"VALUES ('{row.crash_datetime}', {city}, null, "
                        f"'{row.location}', '{row.weather}', {row.total_injury}, {row.total_killed});")
        connect.commit()
        print(f"end insert accidents for {y}", localtime(time()))
    print(f"Records about accidents were inserted successfully", localtime(time()))

if __name__ == '__main__':
    con = connect_postgresql()

    # insert_into_cities(con, 'Chicago')

    spark = SparkSession \
        .builder\
        .getOrCreate()

    # boroughs = spark.read.csv(
    #     SparkFiles.get(Path(dirname(abspath(__file__)), 'boroughs_chicago.csv')),
    #     header=True,
    #     inferSchema=False)
    #
    # insert_into_boroughs(con, boroughs)

    accidents = spark.read.csv(
        SparkFiles.get(Path(dirname(abspath(__file__)), 'chicago_accidents.csv')),
        header=True,
        inferSchema=False)

    accidents.groupBy(year("crash_datetime")).count().show()

    insert_into_accidents(con, accidents)


    # jdbcDF = spark.read \
    #     .format("jdbc") \
    #     .option("url", f"jdbc:postgresql://{config['PostgreSQL']['host']}/{config['PostgreSQL']['database']}") \
    #     .option("dbtable", "cities") \
    #     .option("user", config["PostgreSQL"]["user"]) \
    #     .option("password", config["PostgreSQL"]["password"]) \
    #     .option("driver", "org.postgresql.Driver") \
    #     .load()
    #
    # jdbcDF.show()