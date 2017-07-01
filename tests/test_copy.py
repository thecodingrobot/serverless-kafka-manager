from api.copy import copy_handler
import json, uuid


def test_copying_messages(kafka_producer, context):
    rand = str(uuid.uuid4())[:7]
    from_topic = 'from_topic_' + rand
    to_topic = 'to_topic_' + rand
    messages = ['Message {}'.format(i) for i in range(10)]

    for msg in messages:
        kafka_producer.produce(from_topic, value=msg)
    kafka_producer.flush()

    event = {
        'pathParameters': {
            'from_topic': from_topic,
            'to_topic': to_topic
        }
    }

    result = copy_handler(event, context)

    assert json.loads(result['body'])['copiedMessages'] == len(messages)
