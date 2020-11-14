"""
Show how many events are received, display in a 2-minute tumbling window.
Show result at 1-minute interval.
Use "kafka"source and "console"sink.
Set outputMode to "complete".
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
    .master("yarn")\
    .appName("Jaye_Project")\
    .config("spark.dynamicAllocation.enabled", "false")\
    .config("spark.streaming.backpressure.enabled", "true")\
    .config("spark.streaming.kafka.maxRatePerPartition", 100)\
    .config("spark.sql.shuffle.partitions", 2)\
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "ip-172-31-91-232.ec2.internal:9092")\
    .option("subscribe", "rsvp_jaye")\
    .option("startingOffsets", "latest")\
    .option("failOnDataLoss", "false")\
    .load()


# convert the value column from value to struct type
df2 = df.select('timestamp')

# create the tumbling window
df3 = df2.groupBy(window(col("timestamp"), "2 minutes"))\
    .count()\
    .orderby('window')

df3.writeStream\
    .trigger(processingTime="60 seconds")\
    .queryName("events_per_window1")\
    .format("console")\
    .outputMode("complete")\
    .option("truncate", "false")\
    .start()\
    .awaitTermination()
