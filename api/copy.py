import json
import os
from api import kafka_producer, kafka_consumer, lambda_response


@lambda_response
def copy_handler(event, context):
    from_topic = event['pathParameters']['from_topic']
    to_topic = event['pathParameters']['to_topic']
    settings = {
        'bootstrap.servers': os.environ['KAFKA_HOSTS'],
    }
    kafka_use_ssl = 'True' == os.environ.get('KAFKA_USE_SSL', 'False')

    copied_messages = 0
    with kafka_producer(settings, kafka_use_ssl) as p, kafka_consumer(settings, kafka_use_ssl) as c:
        c.subscribe([from_topic])
        while context.get_remaining_time_in_millis() > 1500:
            msg = c.poll(timeout=1.0)
            if msg:
                if not msg.error():
                    p.produce(to_topic, key=msg.key(), value=msg.value())
                    copied_messages += 1
    return json.dumps({
        'copiedMessages': copied_messages
    })
