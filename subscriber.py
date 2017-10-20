import pika
import logging

from pika import adapters
import json
import traceback

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class RabbitConsumer(object):

    def __init__(self):
        self._connection = self.connect()
        self._channel = None
        self._closing = False
        self._consumer_tag = None

    def connect(self):
        LOGGER.info('Connecting')
        credentials = pika.PlainCredentials('admin', 'admin')
        return adapters.TornadoConnection(pika.ConnectionParameters(host='localhost',port=5672, virtual_host='/',credentials=credentials), self.on_connection_open)

    def close_connection(self):
        LOGGER.info('Closing connection')
        self._connection.close()

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 10 seconds: (%s) %s',
                           reply_code, reply_text)
            self._connection.add_timeout(10, self.reconnect)

    def on_connection_open(self, unused_connection):
        LOGGER.info('Connection opened')
        self._connection.add_on_close_callback(self.on_connection_closed)
        self.open_channel()

    def reconnect(self):
        if not self._closing:
            # Create a new connection
            self._connection = self.connect()

    def on_channel_closed(self, channel, reply_code, reply_text):
        LOGGER.warning('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self._connection.close()

    def setup_queue(self, channel):
        LOGGER.info('Channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self._channel.basic_qos(prefetch_count=5) 

        #Declare Queue
        self._channel.queue_declare(self.queue_bind, queue='my_queue', durable=True)

    def queue_bind(self, method_frame):
        self._channel.queue_bind(self.on_bindok, exchange='my_exchange', queue='my_queue')

    def on_bindok(self, unused_frame):
        self.start_consuming()

    def add_on_cancel_callback(self):
        LOGGER.info('Adding consumer cancellation callback')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        LOGGER.info('Consumer was cancelled remotely, shutting down: %r',
                    method_frame)
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        LOGGER.info(" [x] Received %r" % json.loads(body.decode('UTF-8')))
        
        self._channel.basic_ack(basic_deliver.delivery_tag)

    def on_cancelok(self, unused_frame):
        LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
        self.close_channel()

    def stop_consuming(self):
        if self._channel:
            LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def start_consuming(self):
        LOGGER.info('Issuing consumer related RPC commands')
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self.on_message, queue='my_queue')

    def close_channel(self):
        LOGGER.info('Closing the channel')
        self._channel.close()

    def open_channel(self):
        LOGGER.info('Creating a new channel')
        self._connection.channel(on_open_callback=self.setup_queue)

    def run(self):
        self._connection.ioloop.start()

    def stop(self):
        LOGGER.info('Stopping')
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.stop()
        LOGGER.info('Stopped')


def main():
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    rabbit = RabbitConsumer()
    try:
        rabbit.run()
    except KeyboardInterrupt:
        rabbit.stop()

if __name__ == '__main__':
    main()
