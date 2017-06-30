from contextlib import contextmanager
import logging

from confluent_kafka.avro import AvroConsumer
from confluent_kafka.cimpl import Consumer

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@contextmanager
def kafka_consumer(settings, is_raw=True):
    def error_callback(er):
        logger.error(er)

    config = {'error_cb': error_callback,
              'debug': 'all',
              'security.protocol': 'ssl',
              'ssl.key.location': 'service.key',
              'ssl.certificate.location': 'service.cert',
              'ssl.ca.location': 'ca.pem',
              'api.version.request': True,
              'default.topic.config': {'auto.offset.reset': 'smallest'}
              }

    config.update(settings)
    from pprint import pprint
    pprint(config)
    if is_raw:
        consumer = Consumer(config)
    else:
        consumer = AvroConsumer(config)

    yield consumer

    consumer.close()
