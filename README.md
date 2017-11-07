Python / Kafka Examples for Heroku
---

A small set of examples that show how you can use Kafka on Heroku with Python, from your own personal dev environment.

**Examples**

`standard_consumer.py` and `standard_producer.py` both use the confluent-kafka-python library.

`asyncio_kafka_consumer.py` is a consumer built on the Python 3.5+ Asyncio library.

`asyncio_websockets_kafka_consumer.py` is built on Tornado / Asyncio / aiokafka, 
and shows an example of how to do pubsub with Kafka without blocking, and how to connect it to Tornado which
handles websocket connections.

**How to Use**

I am running this on a Basic-0 Heroku plan.  You need to make sure `HEROKU_STAGING_APP_NAME` is pointing to the application which contains the Kafka plugin.   

If you wish to use it off of Heroku, you can play with the kafka_settings files to make changes to the connection settings.
 