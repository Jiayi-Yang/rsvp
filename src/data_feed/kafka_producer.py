import json
import requests
import logging as log
from src.config import BROKER, TOPIC_NAME, URL
from kafka import KafkaProducer


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(excp):
    log.error('I am an error back', exc_info=excp)
    # handle exception


def kfk_producer(rsvp_data):
    # produce json messages
    producer = KafkaProducer(bootstrap_servers=f'{BROKER}',
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))
    # produce asynchronously with callbacks
    producer.send(f'{TOPIC_NAME}', rsvp_data).add_callback(on_send_success).add_errback(on_send_error)

    # block until all async messages are sent
    producer.flush()


if __name__ == '__main__':
    # # local input
    # with open('/Users/jay/Documents/project/pythonProject/rsvp/meetup_rsvp.json') as f:
    #     rsvp_data = json.load(f)
    #     kfk_producer(rsvp_data)

    # stream input
    response = requests.get(URL, stream=True)
    for line in response.iter_lines():
        if line:
            rsvp_data = json.loads(line)
            kfk_producer(rsvp_data)


