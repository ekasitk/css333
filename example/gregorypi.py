from pyspark.sql import SparkSession

def inverse(i):
    divisor = 2*i+1
    if i % 2 != 0:
         divisor = -divisor
    return 1.0/divisor


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .getOrCreate()

    sc = spark.sparkContext

    arr = range(0,100000)

    dataRdd = sc.parallelize(arr,4)

    newRdd = dataRdd.map(inverse)

    sum = newRdd.reduce(lambda i,j: i+j)

    print(sum*4)

    spark.stop()
