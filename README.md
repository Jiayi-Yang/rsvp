# Stream Data Processing
## Overview
Real-Time Meetup RSPV Data Processing from https://www.meetup.com/
## Project Technical Overview
### Project Technical Overview
These data processing scripts are developed in Python by using:
- Data Feeds: Kafka
- ETL: Spark DataFrame, Spark Structured Streaming
- Data Table: Impala
- Data Storage: Hdfs, Kudu
- Resource Management: Yarn
### Structure
- data_feed: kafka producer and consumer
- stream: pyspark streaming jobs
- configs: all configurations
## Kafka
### Run Kafka in local
- Install latest Kafka
```bash
tar -xzf kafka_2.13-2.6.0.tgz
cd kafka_2.13-2.6.0
```
- Start the ZooKeeper service
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```
- Start the Kafka broker service
```bash
bin/kafka-server-start.sh config/server.properties
```
- Run Python Scripts
```bash
python src/data_feed/kafka_producer.py
python src/data_feed/kafka_consumer.py
```
## Spark
### Run script
```bash
bin/spark-submit — packages org.apache.spark:spark-streaming-kafka-0–8_2.13:2.6.0 stream/json_save.py
```