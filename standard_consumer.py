"""
An example Kafka Consumer that uses the standard (blocking) confluent_kafka.
"""

import os
from tempfile import NamedTemporaryFile

from confluent_kafka import Consumer, KafkaError, Message

from helpers.env import load_heroku_environmental_variables
from helpers.kafka_settings import heroku_kafka_name, get_kafka_brokers


def with_consumer(group_id: str):
    """
    Cleans up files and certs where possible.

    confluent_kafka doesn't work effectively with kafka_helper's SSL Context.

    :param group_id:
    :return:
    """
    group_id = group_id.rstrip()

    def outer_fn(fn):
        def inner_fn(*args, **kwargs):
            load_heroku_environmental_variables()
            with NamedTemporaryFile(suffix='.crt') as cert_file, \
                    NamedTemporaryFile(suffix='.key') as key_file, \
                    NamedTemporaryFile(suffix='.crt') as trust_file:
                cert_file.write(os.environ['KAFKA_CLIENT_CERT'].encode('utf-8'))
                cert_file.flush()

                key_file.write(os.environ['KAFKA_CLIENT_CERT_KEY'].encode('utf-8'))
                key_file.flush()  # Need to add cryptography to hide key on fs.

                trust_file.write(os.environ['KAFKA_TRUSTED_CERT'].encode('utf-8'))
                trust_file.flush()

                consumer_init_vars = {
                    "bootstrap.servers": ','.join(get_kafka_brokers()),
                    "group.id": heroku_kafka_name(group_id),
                    "security.protocol": "ssl",
                    "ssl.certificate.location": cert_file.name,
                    "ssl.ca.location": trust_file.name,
                    "ssl.key.location": key_file.name,
                    "auto.offset.reset": 'earliest',
                    'default.topic.config': {'auto.offset.reset': 'smallest'}
                }
                c = Consumer(**consumer_init_vars)
                kwargs['kafka_consumer'] = c
                fn(*args, **kwargs)
                c.close()
        return inner_fn
    return outer_fn


def on_delivery(msg):
        print('Message delivered to %s [%s] at offset [%s]: %s' %
              (msg.topic(), msg.partition(), msg.offset(), msg.value()))


@with_consumer(group_id='test-jbm')
def consume_blocking(topic, kafka_consumer: Consumer, **kwargs):
    message_handling_fn = kwargs.get('message_handling_fn', lambda msg: print(repr(msg.value().decode('utf8'))))

    print("Starting consumption")
    topic = [heroku_kafka_name(t) for t in topic]
    print("Attempting to consume from {0} using consumer {1}".format(topic, kafka_consumer))
    kafka_consumer.subscribe(topic)

    msg: Message = kafka_consumer.poll()

    while not msg.error() or msg.error().code() == KafkaError._PARTITION_EOF:
        if msg.error() is None:
            message_handling_fn(msg)
        msg = kafka_consumer.poll()

    print(msg.error())


if __name__ == "__main__":
    os.environ.setdefault('HEROKU_STAGING_APP_NAME', 'jbm-lunchforce-staging')
    tn = input("Please enter the name of the topic you wish to consume from: ")
    print("If the topic does not already exist, this will return an error.")

    consume_blocking([tn], message_handling_fn=on_delivery)
