from pathlib import Path
from os.path import dirname, abspath
from datetime import datetime

from geospark.register import GeoSparkRegistrator
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, to_timestamp, when
from pyspark.sql.types import IntegerType

from work_with_document import write_csv, spark_read_csv


def primary_processing(raw_data):
    tmp_sdf = raw_data.select(
        "CRASH DATE",
        "CRASH TIME",
        "BOROUGH",
        "LOCATION",
        "NUMBER OF PERSONS INJURED",
        "NUMBER OF PERSONS KILLED",
        "NUMBER OF PEDESTRIANS INJURED",
        "NUMBER OF PEDESTRIANS KILLED",
        "NUMBER OF CYCLIST INJURED",
        "NUMBER OF CYCLIST KILLED",
        "NUMBER OF MOTORIST INJURED",
        "NUMBER OF MOTORIST KILLED",
    )
    tmp_sdf = (
        tmp_sdf.withColumnRenamed('CRASH DATE', 'crash_date')
        .withColumnRenamed('CRASH TIME', 'crash_time')
        .withColumnRenamed('BOROUGH', 'borough')
        .withColumnRenamed('LOCATION', 'location')
        .withColumnRenamed('NUMBER OF PERSONS INJURED', 'person_injured')
        .withColumnRenamed('NUMBER OF PERSONS KILLED', 'person_killed')
        .withColumnRenamed(
            'NUMBER OF PEDESTRIANS INJURED', 'pedestrian_injured'
        )
        .withColumnRenamed('NUMBER OF PEDESTRIANS KILLED', 'pedestrian_killed')
        .withColumnRenamed('NUMBER OF CYCLIST INJURED', 'cyclist_injured')
        .withColumnRenamed('NUMBER OF CYCLIST KILLED', 'cyclist_killed')
        .withColumnRenamed('NUMBER OF MOTORIST INJURED', 'motorist_injured')
        .withColumnRenamed('NUMBER OF MOTORIST KILLED', 'motorist_killed')
    )
    tmp_sdf = (
        tmp_sdf.withColumn(
            "person_injured", tmp_sdf["person_injured"].cast(IntegerType())
        )
        .withColumn(
            "person_killed", tmp_sdf["person_killed"].cast(IntegerType())
        )
        .withColumn(
            "pedestrian_injured",
            tmp_sdf["pedestrian_injured"].cast(IntegerType()),
        )
        .withColumn(
            "pedestrian_killed",
            tmp_sdf["pedestrian_killed"].cast(IntegerType()),
        )
        .withColumn(
            "cyclist_injured", tmp_sdf["cyclist_injured"].cast(IntegerType())
        )
        .withColumn(
            "cyclist_killed", tmp_sdf["cyclist_killed"].cast(IntegerType())
        )
        .withColumn(
            "motorist_injured", tmp_sdf["motorist_injured"].cast(IntegerType())
        )
        .withColumn(
            "motorist_killed", tmp_sdf["motorist_killed"].cast(IntegerType())
        )
    )
    tmp_sdf = (
        tmp_sdf.withColumn(
            "person_injured",
            when(tmp_sdf.person_injured.isNull(), 0).otherwise(
                tmp_sdf.person_injured
            ),
        )
        .withColumn(
            "person_killed",
            when(tmp_sdf.person_killed.isNull(), 0).otherwise(
                tmp_sdf.person_killed
            ),
        )
        .withColumn(
            "pedestrian_injured",
            when(tmp_sdf.pedestrian_injured.isNull(), 0).otherwise(
                tmp_sdf.pedestrian_injured
            ),
        )
        .withColumn(
            "pedestrian_killed",
            when(tmp_sdf.pedestrian_killed.isNull(), 0).otherwise(
                tmp_sdf.pedestrian_killed
            ),
        )
        .withColumn(
            "cyclist_injured",
            when(tmp_sdf.cyclist_injured.isNull(), 0).otherwise(
                tmp_sdf.cyclist_injured
            ),
        )
        .withColumn(
            "cyclist_killed",
            when(tmp_sdf.cyclist_killed.isNull(), 0).otherwise(
                tmp_sdf.cyclist_killed
            ),
        )
        .withColumn(
            "motorist_injured",
            when(tmp_sdf.motorist_injured.isNull(), 0).otherwise(
                tmp_sdf.motorist_injured
            ),
        )
        .withColumn(
            "motorist_killed",
            when(tmp_sdf.motorist_killed.isNull(), 0).otherwise(
                tmp_sdf.motorist_killed
            ),
        )
    )
    clean_sdf = tmp_sdf.filter(tmp_sdf['location'].isNotNull())

    df_injured = clean_sdf.withColumn(
        "total_injured",
        clean_sdf.person_injured
        + clean_sdf.pedestrian_injured
        + clean_sdf.cyclist_injured
        + clean_sdf.motorist_injured,
    )
    df_killed = df_injured.withColumn(
        "total_killed",
        df_injured.person_killed
        + df_injured.pedestrian_killed
        + df_injured.cyclist_killed
        + df_injured.motorist_killed,
    )
    sdf_datetime = df_killed.withColumn(
        "crash_datetime", concat("crash_date", "crash_time")
    )
    convert_datetime = sdf_datetime.select(
        to_timestamp(sdf_datetime.crash_datetime, "MM/dd/yyyyHH:mm").alias(
            "crash_datetime"
        ),
        "borough",
        "location",
        "person_injured",
        "person_killed",
        "pedestrian_injured",
        "pedestrian_killed",
        "cyclist_injured",
        "cyclist_killed",
        "motorist_injured",
        "motorist_killed",
        "total_injured",
        "total_killed",
    )
    final_sdf = convert_datetime.filter(
        convert_datetime.crash_datetime > "2016-01-01"
    )
    return final_sdf


def add_boroughs(spark, primary_processed_data, boroughs):
    boroughs.createOrReplaceTempView("boroughs")
    boroughs_geom = spark.sql(
        '''
        SELECT BoroName, st_geomFromWKT(the_geom) as borough
        FROM boroughs
        '''
    )
    boroughs_geom.createOrReplaceTempView("boroughs")
    primary_processed_data.createOrReplaceTempView("accidents")
    accidents_geom = spark.sql(
        '''
        SELECT
        crash_datetime, borough,
        st_geomFromWKT(location) as location,
        person_injured, person_killed,
        pedestrian_injured, pedestrian_killed,
        cyclist_injured, cyclist_killed,
        motorist_injured, motorist_killed,
        total_injured, total_killed
        FROM accidents
        '''
    )
    accidents_geom.createOrReplaceTempView("accidents")
    data_mix_boroughs = spark.sql(
        '''
        SELECT
        a.crash_datetime, a.borough as borough_from_raw_data,
        a.location,
        a.person_injured, a.person_killed,
        a.pedestrian_injured, a.pedestrian_killed,
        a.cyclist_injured, a.cyclist_killed,
        a.motorist_injured, a.motorist_killed,
        a.total_injured, a.total_killed,
        b.BoroName as borough
        FROM accidents AS a
        LEFT OUTER JOIN boroughs AS b
        ON ST_Intersects(a.location, b.borough)
        '''
    )
    data_with_boroughs = data_mix_boroughs.withColumn(
        "borough",
        when(
            data_mix_boroughs.borough.isNull(),
            data_mix_boroughs.borough_from_raw_data,
        ).otherwise(data_mix_boroughs.borough),
    )
    return data_with_boroughs


def add_neighborhoods(spark, data_with_boroughs, neighborhoods):
    data_with_boroughs.createOrReplaceTempView("accidents")
    neighborhoods.createOrReplaceTempView("neighborhoods")
    neighborhoods_geom = spark.sql(
        """
        SELECT neighborhood, st_geomFromWKT(polygones) as geo
        FROM neighborhoods
        """
    )
    neighborhoods_geom.createOrReplaceTempView("neighborhoods")
    data_with_boroughs_and_neighborhoods = spark.sql(
        """
        SELECT
        a.crash_datetime,
        'New York' as city, a.borough,
        n.neighborhood,
        st_AsText(a.location) as location,
        a.person_injured, a.person_killed,
        a.pedestrian_injured, a.pedestrian_killed,
        a.cyclist_injured, a.cyclist_killed,
        a.motorist_injured, a.motorist_killed,
        a.total_injured, a.total_killed,
        a.borough_from_raw_data
        FROM accidents AS a
        LEFT OUTER JOIN neighborhoods AS n
        ON ST_Intersects(a.location, n.geo)
        """
    )
    return data_with_boroughs_and_neighborhoods


def prepare_data_about_NY(spark):
    cwd = dirname(abspath(__file__))
    print(f"{datetime.now()} - start primary processing")
    raw_data = spark_read_csv(
        spark,
        Path(cwd, "data_source", "Motor_Vehicle_Collisions_-_Crashes.csv"),
    )
    primary_processed_data = primary_processing(raw_data)

    print(f"{datetime.now()} - end primary processing")

    print(f"{datetime.now()} - start adding boroughs")
    boroughs = spark_read_csv(spark, Path(cwd, "data_source", "nybb.csv"))
    data_with_boroughs = add_boroughs(spark, primary_processed_data, boroughs)
    print(f"{datetime.now()} - end adding boroughs")

    print(f"{datetime.now()} - start adding neighborhoods")
    neighborhoods = spark_read_csv(
        spark, Path(cwd, "data_source", "ny_neighborhoods.csv")
    )
    data_with_boroughs_and_neighborhoods = add_neighborhoods(
        spark, data_with_boroughs, neighborhoods
    )
    print(f"{datetime.now()} - end adding neighborhoods")

    print(f"{datetime.now()} - start create csv")
    write_csv(
        Path(cwd, "resulting_data", "NY.csv"),
        mode="w",
        values=[
            [
                "crash_datetime",
                "city",
                "borough",
                "neighborhood",
                "location",
                "person_injured",
                "person_killed",
                "pedestrian_injured",
                "pedestrian_killed",
                "cyclist_injured",
                "cyclist_killed",
                "motorist_injured",
                "motorist_killed",
                "total_injured",
                "total_killed",
                "borough_from_raw_data",
            ]
        ],
    )
    write_csv(
        Path(cwd, "resulting_data", "NY.csv"),
        mode="a",
        values=data_with_boroughs_and_neighborhoods.collect(),
    )
    print(f"{datetime.now()} - end create csv")


if __name__ == '__main__':
    print(f"{datetime.now()} - start program")
    spark = SparkSession.builder.getOrCreate()
    GeoSparkRegistrator.registerAll(spark)
    prepare_data_about_NY(spark)
    print(f"{datetime.now()} - end program")
