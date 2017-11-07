"""
Run a Tornado non-blocking server which uses the asyncio event loop and aiokafka library to handle Kafka connections
"""

import asyncio
import os

import tornado.httpserver
from aiokafka import AIOKafkaConsumer
from tornado.platform.asyncio import AsyncIOMainLoop

from helpers.env import load_heroku_environmental_variables
from helpers.kafka_settings import get_asyncio_kafka_connector


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", MainHandler),
            (r"/chatsocket", ChatSocketHandler),
        ]
        settings = dict(
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
        )
        super(Application, self).__init__(handlers, **settings)


class MainHandler(tornado.web.RequestHandler):
    """
    Serves a blank text page which then performs websocket ocnnections against /chatsocket
    """
    def get(self):
        self.render("index.html")


class KeepWebsocketListenersAvailable(tornado.websocket.WebSocketHandler):
    listeners = set()

    def open(self, *args, **kwargs):
        super().open(*args, **kwargs)
        print("WebSocket opened")
        self.__class__.listeners.add(self)

    def on_close(self):
        super().on_close()
        print("WebSocket closed")
        self.__class__.listeners.remove(self)


class ChatSocketHandler(KeepWebsocketListenersAvailable):
    """
    Creates a Websocket which adds listeners to the listeners variable.
    We then have the ioloop send messages to this.
    """

    @classmethod
    async def update_all_listeners(cls, whatever):
        print("Updating listeners {0}".format(whatever))
        for l in cls.listeners:
            l.write_message(whatever)

    def on_message(self, message):
        print('Message Received from Websocket: {0}'.format(message))

    def data_received(self, chunk):
        pass


async def get_kafka_messages(kc: AIOKafkaConsumer):
    print('Waiting for kafka start')
    await kc.start()
    print('Started')

    try:
        print('waiting for messages')
        # Consume messages
        async for msg in kc:
            msg = "consumed: {0}, {1}, {2}, {3}, {4}, {5}".format(msg.topic, msg.partition, msg.offset,
                                                                  msg.key, msg.value, msg.timestamp)
            asyncio.ensure_future(ChatSocketHandler.update_all_listeners(msg))
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        print('Stopping consumer')
        await kc.stop()


if __name__ == "__main__":
    os.environ.setdefault('HEROKU_STAGING_APP_NAME', 'jbm-lunchforce-staging')
    load_heroku_environmental_variables()
    AsyncIOMainLoop().install()

    loop = asyncio.get_event_loop()
    consumer = get_asyncio_kafka_connector(loop)

    app = Application()
    server = tornado.httpserver.HTTPServer(app)
    server.bind(12345, '127.0.0.1')
    server.start()

    loop.run_until_complete(get_kafka_messages(consumer))
    loop.close()
