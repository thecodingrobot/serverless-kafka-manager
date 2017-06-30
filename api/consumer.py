import json
import logging
import os
import uuid

from confluent_kafka import KafkaError, TopicPartition
from confluent_kafka.avro.serializer import SerializerError

from api import kafka_consumer

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def consumer_handler(event, context):
    topic = event['pathParameters']['topic']

    if event['queryStringParameters'] is None:
        event['queryStringParameters'] = {}

    group_id = event['queryStringParameters'].get('groupId', str(uuid.uuid1()))
    is_raw = 'raw' in event['queryStringParameters']

    logger.info('Topic: {} Group id {}'.format(topic, group_id))

    settings = {
        'metadata.broker.list': os.environ['KAFKA_HOSTS'],
        'group.id': group_id,
    }
    if not is_raw:
        settings['schema.registry.url'] = os.environ['SCHEMA_REGISTRY']

    with kafka_consumer(settings, is_raw) as consumer:
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
            while context.get_remaining_time_in_millis() < 1500:
                msg = consumer.poll(timeout=1.0)
                if msg:
                    return process_msg(msg, settings, is_raw)

            return {
                'statusCode': 204,
                'body': 'Did not receive any message.'
            }
        except SerializerError as e:
            return {
                'statusCode': 400,
                'body': "Message deserialization failed for {}: {}".format(msg, e)
            }


def process_msg(msg, settings, is_raw=True):
    if not msg.error():
        msg_metadata = {'topic': msg.topic(),
                        'group_id': settings['group.id'],
                        'offset': msg.offset(),
                        'partition': msg.partition(),
                        'key': msg.key()
                        }
        if is_raw:
            from hexdump import hexdump
            msg_metadata['payload'] = hexdump(msg.value(), result='return')
            return {
                'statusCode': 200,
                'headers': {'content-type': 'text/plain'},
                'body': (("Metadata:\n"
                          "Topic:     {m[topic]}\n"
                          "Group ID:  {m[group_id]}\n"
                          "Offset:    {m[offset]}\n"
                          "Partition: {m[partition]}\n"
                          "Key:       {m[key]}\n"
                          "\n"
                          "Message:\n"
                          "{m[payload]}\n"
                          ).format(m=msg_metadata))
            }
        else:
            def default_decoder(obj):
                return obj.decode('utf-8')

            msg_metadata['payload'] = msg.value()
            return {
                'statusCode': 200,
                'body': json.dumps(msg_metadata, default=default_decoder)
            }
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
