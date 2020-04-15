import pyspark
from pyspark.ml.clustering import BisectingKMeans, KMeans
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import monotonically_increasing_id


def _main():
    spark = pyspark.sql.SparkSession \
            .builder \
            .getOrCreate()
    dataset = spark.read.csv("data-1586261378788.csv", header=True)
    only_numbers = dataset.select(['ny_population', 'ny_number_of_accidents_per_neigborhood_with_population'])
    
    for column in only_numbers.columns:
        only_numbers = only_numbers.withColumn(column, only_numbers[column].cast('int'))
    
    assembler = VectorAssembler(
        inputCols=only_numbers.columns,
        outputCol='features')

    only_numbers_with_features = assembler.transform(only_numbers)
    bkm = BisectingKMeans().setK(5).setSeed(2)
    model = bkm.fit(only_numbers_with_features)
    predictions = model.transform(only_numbers_with_features)
    part1 = dataset.withColumn("id", monotonically_increasing_id())
    part2 = predictions.withColumn("id", monotonically_increasing_id())
    part1.join(part2, on='id').select(['ny_neighborhood_with_population',
                                       'ny_population',
                                       'ny_number_of_accidents_per_neigborhood_with_population',
                                       'prediction']).write.csv('classes', header=True)


if __name__ == "__main__":
    _main()
