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


def create_table_accidents(connect):
    cur = connect.cursor()
    cur.execute(
        '''CREATE TABLE IF NOT EXISTS accidents
        (id SERIAL PRIMARY KEY,
        crash_datetime timestamp NOT NULL,
        city varchar NOT NULL,
        borough varchar NOT NULL,
        neighborhood varchar NOT NULL,
        location varchar NOT NULL,
        weather varchar,
        person_injured smallint,
        person_killed smallint,
        pedestrian_injured smallint,
        pedestrian_killed smallint,
        cyclist_injured smallint,
        cyclist_killed smallint,
        motorist_injured smallint,
        motorist_killed smallint,
        total_injured smallint,
        total_killed smallint);'''
    )
    _LOGGER.debug("Table accidents created successfully")
    connect.commit()


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    _LOGGER.debug("start program")
    con = connect_postgresql()
    create_table_accidents(con)
    con.close()
    _LOGGER.debug("end program")
