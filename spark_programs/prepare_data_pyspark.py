from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, to_timestamp
from pyspark.sql.types import IntegerType


def clear():
    spark = SparkSession.builder.master("local[4]") \
        .appName("Prepate_and_load_data") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sdf = spark.read.csv("Motor_Vehicle_Collisions_-_Crashes.csv", inferSchema='true', header=True)  # read from csv

    tmp_sdf = sdf.select("CRASH DATE", "CRASH TIME", "BOROUGH", "LOCATION", "NUMBER OF PERSONS INJURED",
                         "NUMBER OF PERSONS KILLED", "NUMBER OF PEDESTRIANS INJURED", "NUMBER OF PEDESTRIANS KILLED",\
                         "NUMBER OF CYCLIST INJURED", "NUMBER OF CYCLIST KILLED", "NUMBER OF MOTORIST INJURED",\
                         "NUMBER OF MOTORIST KILLED")

    tmp_sdf = tmp_sdf.withColumnRenamed('CRASH DATE', 'crash_date').withColumnRenamed('CRASH TIME', 'crash_time'). \
        withColumnRenamed('BOROUGH', 'borough').withColumnRenamed('LOCATION', 'location'). \
        withColumnRenamed('NUMBER OF PERSONS INJURED', 'person_injured'). \
        withColumnRenamed('NUMBER OF PERSONS KILLED', 'person_killed'). \
        withColumnRenamed('NUMBER OF PEDESTRIANS INJURED', 'pedestrian_injured'). \
        withColumnRenamed('NUMBER OF PEDESTRIANS KILLED', 'pedestrian_killed'). \
        withColumnRenamed('NUMBER OF CYCLIST INJURED', 'cyclist_injured'). \
        withColumnRenamed('NUMBER OF CYCLIST KILLED', 'cyclist_killed'). \
        withColumnRenamed('NUMBER OF MOTORIST INJURED', 'motorist_injured'). \
        withColumnRenamed('NUMBER OF MOTORIST KILLED', 'motorist_killed')

    tmp_sdf = tmp_sdf.withColumn("person_injured", tmp_sdf["person_injured"].cast(IntegerType())). \
        withColumn("cyclist_killed", tmp_sdf["cyclist_killed"].cast(IntegerType())). \
        withColumn("motorist_injured", tmp_sdf["motorist_injured"].cast(IntegerType()))

    clean_sdf = tmp_sdf.filter(sdf['location'].isNotNull())

    df_injured = clean_sdf.withColumn("total_injured", clean_sdf.person_injured + clean_sdf.pedestrian_injured + \
                                      clean_sdf.cyclist_injured + clean_sdf.motorist_injured)
    df_killed = df_injured.withColumn("total_killed", df_injured.person_killed + df_injured.pedestrian_killed + \
                                      df_injured.cyclist_killed + df_injured.motorist_killed)

    sdf_datetime = df_killed.withColumn("date_time", concat("crash_date", "crash_time"))
    convert_datetime = sdf_datetime.select(to_timestamp(sdf_datetime.date_time, 'MM/dd/yyyyHH:mm').alias('date_time'),
                                           'borough', 'location', 'total_injured', 'total_killed', )

    final_sdf = convert_datetime.filter(convert_datetime.date_time > '2016-01-08')

    return final_sdf.toPandas().to_csv('check.csv', index=False)


if __name__ == '__main__':
    clear()
