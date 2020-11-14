"""
Use impala to create a KUDU table.
Do dataframe transformation to extract information and write to the KUDU table.
Use "kafka"source and "kudu"sink.
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

df1 = df.select(df['value'].cast("string"))

# get schema from a static file
test = spark.read.json("/user/jiayiyang/rsvp/meetup_rsvp.json")
schema = test.schema

# convert the value column from value to struct type
df2 = df1.select(from_json(df1['value'], schema).alias("json"))

# select all columns from the converted column
df3 = df2.select("json.*")

# convert time from unix millisecond to timestamp
df6 = df3.withColumn("event_time", df3["event.time"])\
    .select("rsvp_id", "member.member_id", "member.member_name",
            "group.group_id", "group.group_name", "group.group_city", "group.group_country",
            "event.event_name", "event_time")

# find rows missing data
df6.writeStream.format("console").start()

# filter out missing data
df7 = df6.where(col("rsvp_id").isNotNull())

# write to kudu master
df7.writeStream.trigger(processingTime="60 seconds").format("kudu")\
    .option("kudu.master",
            "ip-172-31-89-172.ec2.internal:7051,ip-172-31-86-198.ec2.internal:7051,ip-172-31-93-228.ec2.internal:7051")\
    .option("kudu.table", "impala::rsvp_db.rsvp_kudu_jaye")\
    .option("kudu.operation", "upsert")\
    .option("checkpointLocation", "/user/jiayiyang/rsvp/kudu_checkpoint/checkpoint0")\
    .outputMode("append")\
    .start()\
    .awaitTermination()
