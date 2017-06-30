from contextlib import contextmanager
import logging, uuid

from confluent_kafka import Producer, Consumer
from confluent_kafka.avro import AvroConsumer

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@contextmanager
def kafka_consumer(settings, is_raw=True):
    def error_callback(er):
        logger.error(er)

    config = {'error_cb': error_callback,
              'security.protocol': 'ssl',
              'ssl.key.location': 'service.key',
              'ssl.certificate.location': 'service.cert',
              'ssl.ca.location': 'ca.pem',
              'api.version.request': True,
              'default.topic.config': {'auto.offset.reset': 'smallest'}
              }

    config.update(settings)

    if 'group.id' not in config:
        config['group.id'] = uuid.uuid4()

    if is_raw:
        consumer = Consumer(config)
    else:
        consumer = AvroConsumer(config)

    yield consumer

    consumer.close()


@contextmanager
def kafka_producer(settings: dict) -> Producer:
    config = {'security.protocol': 'ssl',
              'ssl.key.location': 'service.key',
              'ssl.certificate.location': 'service.cert',
              'ssl.ca.location': 'ca.pem',
              'api.version.request': True,
              }

    config.update(settings)
    producer = Producer(config)
    yield producer
    producer.flush()
