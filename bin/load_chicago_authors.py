

from pyspark import SparkContext
from pyspark.sql import SparkSession

from lindex import ChicagoAuthor


if __name__ == '__main__':

    sc = SparkContext()
    spark = SparkSession(sc).builder.getOrCreate()

    rows = ChicagoAuthor.from_csv('data/chicago/AUTHORS_METADATA.csv')

    df = spark.createDataFrame(rows, ChicagoAuthor.schema)
    df.write.parquet('authors.parquet')
