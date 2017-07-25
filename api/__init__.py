from contextlib import contextmanager
import logging, uuid

from confluent_kafka import Producer, Consumer
from confluent_kafka.avro import AvroConsumer

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_response(func):
    def wrapper(*args):
        result = func(*args)

        if type(result) is dict:
            return result

        return {
            'statusCode': 200,
            'body': result
        }

    return wrapper


@contextmanager
def kafka_consumer(settings, avro=False, use_ssl=False) -> Consumer:
    def error_callback(er):
        logger.error(er)

    config = {'error_cb': error_callback,
              'api.version.request': True,
              'default.topic.config': {'auto.offset.reset': 'smallest'}
              }
    config.update(settings)

    if use_ssl:
        config.update({'security.protocol': 'ssl',
                       'ssl.key.location': 'service.key',
                       'ssl.certificate.location': 'service.cert',
                       'ssl.ca.location': 'ca.pem'})

    if 'group.id' not in config:
        config['group.id'] = uuid.uuid4()

    if avro:
        consumer = AvroConsumer(config)
    else:
        consumer = Consumer(config)

    yield consumer

    consumer.close()


@contextmanager
def kafka_producer(settings: dict, use_ssl=False) -> Producer:
    config = {
        'api.version.request': True,
    }

    if use_ssl:
        config.update({'security.protocol': 'ssl',
                       'ssl.key.location': 'service.key',
                       'ssl.certificate.location': 'service.cert',
                       'ssl.ca.location': 'ca.pem'})

    config.update(settings)
    producer = Producer(config)
    yield producer
    producer.flush()
