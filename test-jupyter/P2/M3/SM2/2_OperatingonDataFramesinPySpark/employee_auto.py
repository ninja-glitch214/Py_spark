
from pyspark.sql import SparkSession



spark = SparkSession. \
    builder. \
    enableHiveSupport(). \
    appName('Demo'). \
    master('local[*]'). \
    getOrCreate()

sc = spark.sparkContext


data = [("Engineering","Alice",75000,5000),
        ("Engineering","Bob",85000,7000),
        ("Engineering","Alice",85000,7000),
        ("Sales","Charlie",60000,3000),
        ("Sales","David",55000,4000)]

columns = ["department","name","salary","bonus"]

data_df = spark.createDataFrame(data,schema=columns)


result_df = data_df.groupBy("department").agg({
    "name":"count",
    "salary":"avg",
    "bonus":"sum"
})

result_df.show()