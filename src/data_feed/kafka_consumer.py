from kafka import KafkaConsumer
from src.config import TOPIC_NAME, GROUP_NAME, BROKER


def kfk_consumer():
    # To consume latest messages and auto-commit offsets
    consumer = KafkaConsumer(f'{TOPIC_NAME}',
                             group_id=f'{GROUP_NAME}',
                             bootstrap_servers=[f'{BROKER}'])
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s:%d:%d: key=%s value=%s" % (message.topic,
                                             message.partition,
                                             message.offset,
                                             message.key,
                                             message.value))


if __name__ == '__main__':
    kfk_consumer()

