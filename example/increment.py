from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .getOrCreate()

    sc = spark.sparkContext


    dataRdd = sc.textFile("/home/student/css333/dataset/ones.txt",2)

    newRdd = dataRdd.map(lambda str: int(str) + 1)

    sum = newRdd.reduce(lambda i,j: i+j)
    print("Summation of all numbers = %d" % sum)

    print("Save newRdd to file")
    newRdd.saveAsTextFile("/home/student/css333/example/newRDD")

    spark.stop()
