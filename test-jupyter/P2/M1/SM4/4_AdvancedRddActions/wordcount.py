
from pyspark.sql import SparkSession



spark = SparkSession. \
    builder. \
    enableHiveSupport(). \
    appName('Demo'). \
    master('local[*]'). \
    getOrCreate()

sc = spark.sparkContext



print(sc.textFile("constitution.txt").flatMap(lambda x: x.split())\
.map(lambda w: (w, 1)).reduceByKey(lambda x, y: x + y).collect())