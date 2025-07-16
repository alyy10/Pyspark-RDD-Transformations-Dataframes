from pyspark.sql import SparkSession

def create_session():
    #creates spark session
    spark = SparkSession.builder.appName('spark-app').getOrCreate()
    return spark

def load_data(spark):
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(
        "trip_yellow_taxi.csv")
    #df.show(5)

    df.createOrReplaceTempView("taxi_data")
    spark.sql("select * from taxi_data where VendorID='2' and tpep_pickup_datetime='2017-10-01 00:15:30' and tpep_dropoff_datetime ='2017-10-01 00:25:11' and passenger_count='1'").show(5)
    df.filter(df.VendorID=='2').filter(df.tpep_pickup_datetime == '2017-10-01 00:15:30').filter(df.tpep_dropoff_datetime =='2017-10-01 00:25:11').filter(df.passenger_count == '1').show(5)
    #& df.tpep_pickup_datetime == '2017-10-01 00:15:30' & df.tpep_dropoff_datetime == '2017-10-01 00:25:11' & df.passenger_count == '1')
    spark.sql("select * from taxi_data where RatecodeID=4").show(5)
    df.filter(df.RatecodeID==4).show(5)
    spark.sql("select payment_type, count(*) as payment_type_count from taxi_data group by payment_type ").show(5)
    df.groupBy(df.payment_type).sum("payment_type").show()

def main():
    spark_sess = create_session()
    load_data(spark_sess)

main()
