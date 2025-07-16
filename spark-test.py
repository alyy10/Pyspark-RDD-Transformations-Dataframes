from pyspark.sql import SparkSession

def create_session():
    #creates spark session
    spark = SparkSession.builder.appName('spark-app').getOrCreate()
    return spark

def load_data(spark):
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        "/Users/ali/Downloads/Spark_Training/HospInfo.csv")
    df.show(5)
    #Registering Temp View
    df.createOrReplaceTempView("hosp_data")
    #Applying SQL query to extract result
    spark.sql("select * from hosp_data where City='BOAZ'").show(5)
    df.filter(df.City =='BOAZ').show()

def main():
    spark_sess = create_session()
    load_data(spark_sess)

main()


#spark-submit --deploy-mode client --master "local[2]" --py-files /Users/arunkumarn/PycharmProjects/pyspark-submit/spark-test.py spark-test.py