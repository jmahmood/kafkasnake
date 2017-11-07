import os
from tempfile import NamedTemporaryFile

from confluent_kafka import Producer

from helpers.env import load_heroku_environmental_variables
from helpers.kafka_settings import get_kafka_brokers, heroku_kafka_name


def with_producer(fn):
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

            p = Producer(**{
                "bootstrap.servers": ','.join(get_kafka_brokers()),
                "security.protocol": "ssl",
                "ssl.certificate.location": cert_file.name,
                "ssl.ca.location": trust_file.name,
                "ssl.key.location": key_file.name})

            kwargs['kafka_producer'] = p
            fn(*args, **kwargs)
            p.poll(0)
            p.flush()
    return inner_fn


def on_delivery(err, msg):
    if err:
        print('Message delivery failed (%s [%s]): %s' %
              (msg.topic(), str(msg.partition()), err))
    else:
        print('Message delivered to %s [%s] at offset [%s]: %s' %
              (msg.topic(), msg.partition(), msg.offset(), msg.value()))


@with_producer
def produce(topic, message, kafka_producer: Producer, **kwargs):
    kafka_producer.produce(
        heroku_kafka_name(topic),  # The actual name is the topic name with the prefix from KAFKA_PREFIX
        message, on_delivery=on_delivery)


if __name__ == "__main__":
    tn = input("Please enter the name of the topic you wish to post to: ")
    print("If the topic does not already exist, this will return an error.")
    tm = input("Please enter the message you want to send to the topic: ")
    produce(tn, tm)
