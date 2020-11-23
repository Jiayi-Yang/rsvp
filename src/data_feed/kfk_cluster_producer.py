import json
import requests
import logging as log
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
    producer = KafkaProducer(bootstrap_servers='ip-172-31-91-232.ec2.internal:9092',
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))
    # produce asynchronously with callbacks
    producer.send('rsvp_jaye', rsvp_data).add_callback(on_send_success).add_errback(on_send_error)

    # block until all async messages are sent
    producer.flush()


if __name__ == '__main__':
    # stream input
    response = requests.get('https://stream.meetup.com/2/rsvps', stream=True)
    for line in response.iter_lines():
        if line:
            rsvp_data = json.loads(line)
            kfk_producer(rsvp_data)


