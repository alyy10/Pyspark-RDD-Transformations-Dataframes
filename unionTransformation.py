from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('coalesceTransformation').getOrCreate()
sc = spark.sparkContext
import os
path = os.path.join("/Users/ali/Downloads/Spark_Training", "union-text.txt")
with open(path, "w") as testFile:
    testFile.write("Hello")
textFile = sc.textFile(path)
textFile.collect()
parallelized = sc.parallelize(["World!"])
sorted(sc.union([textFile, parallelized]).collect())