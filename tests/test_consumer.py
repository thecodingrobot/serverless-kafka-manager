import uuid

from api.consumer import consumer_handler


def test_consume_hex_message_on_empty_queue(kafka, context):
    event = {
        'pathParameters': {
            'topic': uuid.uuid4()
        },
        'queryStringParameters': {
            'hex': None
        }
    }
    result = consumer_handler(event, context)

    assert result == {
        "statusCode": 204,
        "body": "Did not receive any message."
    }


def test_consume_hex_message(kafka_producer, context):
    topic = 'test_consume_hex_message_topic'
    original_key = 'my_key'
    original_msg = 'HelloWorld'

    kafka_producer.produce(topic, key=original_key, value=original_msg)
    kafka_producer.flush()

    event = {
        'pathParameters': {
            'topic': topic
        },
        'queryStringParameters': {
            'hex': None
        }
    }
    result = consumer_handler(event, context)

    assert result['statusCode'] == 200
    assert 'Metadata' in result['body']
    assert original_key in result['body']
    assert original_msg in result['body']


def test_consume_raw_message(kafka_producer, context):
    topic = 'test_consume_raw_message_topic'
    original_key = 'my_key'
    original_msg = 'HelloWorld'

    kafka_producer.produce(topic, key=original_key, value=original_msg)
    kafka_producer.flush()

    event = {
        'pathParameters': {
            'topic': topic
        },
        'queryStringParameters': {
            'raw': None
        }
    }
    result = consumer_handler(event, context)

    assert result['statusCode'] == 200
    assert original_msg == result['body']
