import psycopg2
from postgis.psycopg import register
from postgis import MultiPolygon, Point
from pathlib import Path
from os.path import dirname, abspath
from pyspark import SparkContext, SparkFiles
from pyspark.sql import *
from create_table import connect_postgresql

def insert_into_cities(connect, values):
    cur = connect.cursor()
    cur.execute(f"SELECT city FROM cities WHERE city = '{values}'")
    if cur.fetchall() == []:
        cur.execute(
            f"INSERT INTO cities (city) "
            f"VALUES ('{values}');"
        )
        print("Record about cities inserted successfully")
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
    print("Record about boroughs inserted successfully")

def insert_into_accidents(connect):
    cur = connect.cursor()
    # for record in values.collect():
    cur.execute(
        f"INSERT INTO accidents (crash_datetime, city, borough, location, weather, total_injury, total_killed)"
        f"VALUES ('2020-03-03 00:00:00', 1, 1, 'POINT (-73.87261 40.68992)', 'clear', 5, 5);"
    )
    connect.commit()
    print("Record about accidents inserted successfully")

if __name__ == '__main__':
    con = connect_postgresql()
    insert_into_cities(con, 'New York')
    sc = SparkContext()
    spark = SparkSession \
        .builder\
        .getOrCreate()
    # sqlContext = SQLContext(sc)
    # boroughs = spark.read.csv(
    #     SparkFiles.get(Path(dirname(abspath(__file__)), 'nybb.csv')),
    #     header=True,
    #     inferSchema=False)
    # insert_into_boroughs(con, boroughs)
    insert_into_accidents(con)







