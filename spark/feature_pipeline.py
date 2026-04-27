from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,col, avg,count,max,min,
    window,current_timestamp
)

from pyspark.sql.types import (
    StructType, StructField, StringType,DoubleType, LongType
)

spark = SparkSession.builder \
    .appName("StreamML-Featureieline") \
    .master("spark://localhost:7077") \
    .config ("spark.jars.ackages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
    .getOrCreate() 
spark.sparkContext.setLogLevel('Warn')  #reduce noise

transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id",        StringType()),
    StructField("amount",         DoubleType()),
    StructField("merchant",       StringType()),
    StructField("timestamp",      DoubleType()),

])

#read from kafka  as a streaming dataframe
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",
            "localhost:9092,localhost:9093,localhost:9094") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# json message parsing
transactions = raw_stream.select(
    from_json(
        col("value").cast("string"),
        transaction_schema
    ).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

#features of fraud detection
features = transactions \
    .withWatermark("kafka_timestamp", "2 minutes") \
    .groupBy(
        col("user_id"),
        window(col("kafka_timestamp"), "10 minutes", "2 minutes")
    ).agg(
        count("transaction_id").alias("txn_count_10min"), #how many transaction done
        avg("amount").alias("avg_amount_10min"), #avgerage time send 
        max("amount").alias("max_amount_10min"), #max single tranaction
        min("amount").alias("min_amount_10min"),
    )

query = features.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

print("Feature pipeline running. Waiting for transactions...")
query.awaitTermination()





























































