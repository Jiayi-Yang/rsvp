from .spark_config import sparkSession as spark
from src.configs import *

# Save events in HDFS in text(json)format.  Use "kafka"source and "file"sink.  Set outputMode to "append".
df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", f"{BROKER}:{PORT}")\
    .option("subscribe", f"{TOPIC_NAME}")\
    .option("startingOffsets", "latest")\
    .option("failOnDataLoss", "false")\
    .load()

df1 = df.select(df['value'].cast("string"))

df1.writeStream\
    .trigger(processingTime="60 seconds")\
    .format("json")\
    .option("path", f"{SINK}/json")\
    .option("checkpointLocation", f"{CHECK_POINT}/checkpoint0")\
    .outputMode("append").start()
