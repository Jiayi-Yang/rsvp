"""
Show how many events are received for each country,
display it in a sliding window(set windowDuration to 3 minutes and slideDuration to 1 minutes).
Show result at 1-minute interval.
Use "kafka" source and "console" sink.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("yarn") \
    .appName("Jaye_Project") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", 100) \
    .config("spark.sql.shuffle.partitions", 2) \
    .getOrCreate()

spark.sparkContext.setLogLevel("INFO")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "ip-172-31-91-232.ec2.internal:9092") \
    .option("subscribe", "rsvp_jaye") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df1 = df.select(col('timestamp'), df['value'].cast("string"))

# get schema from a static file
test = spark.read.json("/user/jiayiyang/rsvp/meetup_rsvp.json")
schema = test.schema

# convert the value column from value to struct type
df2 = df1.select(from_json(df1['value'], schema).alias("json"), 'timestamp')

# select all columns from the converted column
df3 = df2.select("timestamp", "json.group.group_country")
df4 = df3.groupBy(window(col("timestamp"), "3 minutes", "60 seconds"), col("group_country"), ) \
    .count() \
    .orderBy('window')

df4.writeStream \
    .trigger(processingTime="60 seconds") \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", "false") \
    .start() \
    .awaitTermination()
