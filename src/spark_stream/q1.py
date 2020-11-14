"""
Save events in HDFS in text(json)format.
Use "kafka"source and "file"sink.
Set outputMode to "append".
"""
from pyspark.sql import SparkSession

"""Build Spark Session
Configs
----------
dynamicAllocation: false
    YARN resource static allocation resource
backpressure: true
    default false, stream job change to true
maxRatePerPartition: 100
    max 100 kafka events per partition
shuffle.partitions: 2
    default 200, change to 2 due to small dataset and small cluster(12CPU 24Memory)     
"""
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

df1.writeStream\
    .trigger(processingTime="60 seconds")\
    .format("json")\
    .option("path", "/user/jiayiyang/rsvp/rsvp_sink")\
    .option("checkpointLocation", "/user/jiayiyang/rsvp/rsvp_checkpoint")\
    .outputMode("append")\
    .start()\
    .awaitTermination()

