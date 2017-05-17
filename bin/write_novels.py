

from pyspark import SparkContext
from pyspark.sql import SparkSession

from lindex import ChicagoNovel


if __name__ == '__main__':

    sc = SparkContext()
    spark = SparkSession(sc).builder.getOrCreate()

    rows = [

        ChicagoNovel(
            book_id=i,
            filename='filename %d' % i,
            title='title %d' % i,
            auth_last='auth_last %d' % i,
            auth_first='auth_first %d' % i,
            auth_id='auth_id %d' % i,
            publ_city='publ_city %d' % i,
            publisher='publisher %d' % i,
            publ_date=1900+i,
            source='source %d' % i,
            nationality='nationality %d' % i,
            genre='genre %d' % i,
            text='text %d' % i,
        )

        for i in range(1000)

    ]

    df = spark.createDataFrame(rows, ChicagoNovel.schema)
    df.write.parquet('novels.parquet')
