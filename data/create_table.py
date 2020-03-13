import psycopg2
from postgis.psycopg import register
from postgis import MultiPolygon, Point


def connect_postgresql():
  con = psycopg2.connect(
    database="vehicleaccidents",
    user="master",
    password="adminvehicleaccidents",
    host="vehicleaccidents.ci1cdsczdohb.us-east-2.rds-preview.amazonaws.com",
    port="5432"
  )
  print("Database opened successfully")
  return con

def create_extension_postgis(connect):
    cur = connect.cursor()
    cur.execute("SELECT extname FROM pg_extension WHERE extname = 'postgis';")
    if cur.fetchall() == []:
        cur.execute('''CREATE EXTENSION postgis;''')
        print("EXTENSION postgis created successfully")
    connect.commit()
    register(connect)

def create_table_cities(connect):
  cur = connect.cursor()
  cur.execute('''CREATE TABLE IF NOT EXISTS cities
       (id SERIAL PRIMARY KEY,
       city varchar NOT NULL,
       UNIQUE(city));''')
  print("Table cities created successfully")
  connect.commit()

def create_table_boroughs(connect):
  cur = connect.cursor()
  cur.execute('''CREATE TABLE IF NOT EXISTS boroughs
       (id SERIAL PRIMARY KEY,
       city integer NOT NULL REFERENCES cities (id) ON DELETE CASCADE ON UPDATE CASCADE,
       borough varchar NOT NULL,
       geom GEOGRAPHY(MultiPolygon) NOT NULL);''')
  print("Table boroughs created successfully")
  connect.commit()

def create_table_accidents(connect):
  cur = connect.cursor()
  cur.execute("SELECT typname FROM pg_type WHERE typname = 'type_of_weather'")
  if cur.fetchall() == []:
      cur.execute('''CREATE TYPE type_of_weather AS ENUM 
                     ('clear', 'rain', 'snowfall', 'hail', 'fog', 'tornado', 
                    'strong wind', 'snow', 'ice', 'cloudy', 'rainbow', 'drizzle');''')
      print("TYPE type_of_weather created successfully")
  cur.execute('''CREATE TABLE IF NOT EXISTS accidents  
       (id SERIAL PRIMARY KEY,
       crash_datetime timestamp NOT NULL,
       city integer NOT NULL REFERENCES cities (id) ON DELETE CASCADE ON UPDATE CASCADE,
       borough integer NOT NULL REFERENCES boroughs (id) ON DELETE CASCADE ON UPDATE CASCADE,
       location GEOGRAPHY(Point) NOT NULL,
       weather type_of_weather,
       total_injury smallint,
       total_killed smallint);''')
  print("Table accidents created successfully")
  connect.commit()


if __name__ == '__main__':
    con = connect_postgresql()

    create_extension_postgis(con)

    create_table_cities(con)
    create_table_boroughs(con)
    create_table_accidents(con)

    con.close()