"""
Use Heroku Kafka locally with the Asyncio library, and with aiokafka (the Asyncio-enabled library which does not block / do polling)
"""

import asyncio
import os

from aiokafka import AIOKafkaConsumer

from helpers.env import load_heroku_environmental_variables
from helpers.kafka_settings import get_asyncio_kafka_connector


async def get_kafka_messages(kafka_asyncio_consumer: AIOKafkaConsumer):
    """
    Connects to broker and joins consumer group + follows topic.
    
    Outputs 
    :param kafka_asyncio_consumer: 
    :return: 
    """
    # Get cluster layout and join group
    print('Waiting for kafka start')
    await kafka_asyncio_consumer.start()
    print('Started')

    try:
        print('waiting for messages')
        # Consume messages
        async for msg in kafka_asyncio_consumer:
            print(repr(msg))
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        print('Stopping consumer')
        await kafka_asyncio_consumer.stop()


if __name__ == "__main__":
    os.environ.setdefault('HEROKU_STAGING_APP_NAME', 'jbm-lunchforce-staging')
    load_heroku_environmental_variables()
    print('Starting loop')
    loop = asyncio.get_event_loop()
    consumer = get_asyncio_kafka_connector(loop, group_id_name='test-jbm')

    loop.run_until_complete(get_kafka_messages(consumer))
    loop.close()
