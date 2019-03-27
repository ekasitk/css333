from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .getOrCreate()

    sc = spark.sparkContext

    lines = sc.textFile("/home/student/css333/dataset/accesslog.csv",4);

    ips = lines.map(lambda str: (str.split(",")[0],1) )

    counts = ips.reduceByKey(lambda i,j: i+j )

    output = counts.collect()
    for (ip, count) in output:
        print("%s: %i" % (ip, count))

    spark.stop()
