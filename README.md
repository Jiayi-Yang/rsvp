# Data
Real Time Meetup RSPV Data from https://www.meetup.com/
# Kafka
## If using local Kafka
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
# Spark
## Install Pipenv(Optional)
```bash
brew install pipenv
```
## Open the pipenv
```bash
cd rsvp
pipenv install
pipenv shell
```
## Run script
```bash
python -m src.loader
```