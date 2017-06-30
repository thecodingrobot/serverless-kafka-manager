import logging
import os
import uuid

from confluent_kafka import KafkaError, TopicPartition
from confluent_kafka.avro.serializer import SerializerError

from api import kafka_consumer, formatter, lambda_response

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@lambda_response
def consumer_handler(event, context):
    topic = event['pathParameters']['topic']

    if event['queryStringParameters'] is None:
        event['queryStringParameters'] = {}

    group_id = event['queryStringParameters'].get('groupId', str(uuid.uuid1()))

    logger.info('Topic: {} Group id {}'.format(topic, group_id))

    settings = {
        'metadata.broker.list': os.environ['KAFKA_HOSTS'],
        'group.id': group_id,
    }

    is_avro = False
    if 'hex' in event['queryStringParameters']:
        msg_formatter = formatter.HexdumpFormatter(settings)
    elif 'json' in event['queryStringParameters']:
        settings['schema.registry.url'] = os.environ['SCHEMA_REGISTRY']
        is_avro = True
        msg_formatter = formatter.JsonFormatter(settings)
    else:
        msg_formatter = formatter.RawFormatter(settings)

    with kafka_consumer(settings, is_avro) as consumer:
        if 'partition' in event['pathParameters']:
            options = {
                'topic': topic,
                'partition': int(event['pathParameters']['partition'])
            }
            if 'offset' in event['pathParameters']:
                options['offset'] = int(event['pathParameters']['offset'])

            consumer.assign([TopicPartition(**options)])
        else:
            consumer.subscribe([topic])

        try:
            while context.get_remaining_time_in_millis() > 1500:
                msg = consumer.poll(timeout=1.0)
                if msg:
                    if not msg.error():
                        return msg_formatter.format_message(msg)
                    elif msg.error().code() == KafkaError._PARTITION_EOF:
                        return {
                            'statusCode': 204,
                            'body': "Topic {} [{}] reached end at offset {}".format(msg.topic(), msg.partition(),
                                                                                    msg.offset())
                        }
                    else:
                        logger.error('Unhandled kafka error: {}. Settings: {}'.format(msg.error(), settings))
                        return {
                            'statusCode': 204,
                            'body': 'Unhandled kafka error: {}'.format(msg.error())
                        }

            return {
                'statusCode': 204,
                'body': 'Did not receive any message.'
            }
        except SerializerError as e:
            return {
                'statusCode': 400,
                'body': "Message deserialization failed. Reason: {}".format(e)
            }
