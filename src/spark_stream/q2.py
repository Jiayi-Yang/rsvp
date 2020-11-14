"""
Save events in HDFS in parquet format with schema.
Use "kafka"source and "file"sink.
Set outputMode to "append".
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

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "ip-172-31-91-232.ec2.internal:9092")\
    .option("subscribe", "rsvp_jaye")\
    .option("startingOffsets", "latest")\
    .option("failOnDataLoss", "false")\
    .load()

df1 = df.select(df['value'].cast("string"))
df1.printSchema()

# get schema from a static file
test = spark.read.json("/user/jiayiyang/rsvp/meetup_rsvp.json")
schema = test.schema

# convert the value column from value to struct type
df2 = df1.select(from_json(df1['value'], schema).alias("json"))

# select all columns from the converted column
# need to change the checkpoint path each time
df3 = df2.select("json.*")
df3.printSchema()
df3.writeStream.trigger(processingTime="60 seconds")\
    .format("parquet")\
    .option("path", "/user/jiayiyang/rsvp/rsvp_sink/parquet")\
    .option("checkpointLocation", "/user/jiayiyang/rsvp/rsvp_checkpoint/checkpoint1")\
    .outputMode("append")\
    .start()\
    .awaitTermination()



