import json
import pika
import time
import logging
import traceback
import sys

from datetime import datetime

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)

class RabbitPublisher(object):

    def __init__(self):
        credentials = pika.PlainCredentials('admin', 'admin')
        self._params = pika.ConnectionParameters(
            host='localhost',
            port=5672,
            virtual_host='/',
            credentials=credentials)
        self._conn = None
        self._channel = None

    def connect(self):
        if not self._conn or self._conn.is_closed:
            LOGGER.info('\nConnecting....\n\n')
            self._conn = pika.BlockingConnection(self._params)
            self._channel = self._conn.channel()
            
    def _publish(self, package):
        self._channel.basic_publish(exchange='my_exchange',
            routing_key='',
            body=json.dumps(package),
            properties=pika.BasicProperties(
                delivery_mode = 2, # make message persistent
                content_type = 'application/json'
            )
        )

    def publish(self, package):
        """Publish package, reconnecting if necessary."""
        try:
            self.connect()
            self._publish(package)
        except (pika.exceptions.IncompatibleProtocolError, pika.exceptions.ConnectionClosed):
            LOGGER.info('reconnecting to queue in 5 sec...')
            time.sleep(5)
            self.publish(package)

    def close(self):
        LOGGER.info('closing queue connection')
        self._conn.close()


def main():
    logging.basicConfig(level=logging.ERROR, format=LOG_FORMAT)
    rabbit = RabbitPublisher()
    package = {'time' : datetime.now().isoformat(), 'mesasge': ' '.join(sys.argv[1:])}

    try:
        rabbit.publish(package)

    except:
        LOGGER.error(" [x] NOT Sent %r" % json.dumps(package))
        LOGGER.error(traceback.format_exc())

    rabbit.close()

if __name__ == '__main__':
    main()