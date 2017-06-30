from abc import ABCMeta, abstractmethod


class AbstractFormatter(metaclass=ABCMeta):
    def __init__(self, kafka_settings):
        self.kafka_settings = kafka_settings

    @abstractmethod
    def format_message(self, msg):
        pass


class RawFormatter(AbstractFormatter):
    def format_message(self, msg):
        return {
            'statusCode': 200,
            'headers': {'content-type': 'text/plain'},
            'body': msg.value().decode('utf-8')
        }


class JsonFormatter(AbstractFormatter):
    @staticmethod
    def default_decoder(obj):
        return obj.decode('utf-8')

    def format_message(self, msg):
        import json
        data = {'topic': msg.topic(),
                'group_id': self.kafka_settings['group.id'],
                'offset': msg.offset(),
                'partition': msg.partition(),
                'key': msg.key(),
                'payload': msg.value()
                }
        body = json.dumps(data, default=self.default_decoder)
        return {
            'statusCode': 200,
            'body': body
        }


class HexdumpFormatter(AbstractFormatter):
    def format_message(self, msg):
        from hexdump import hexdump
        data = {'topic': msg.topic(),
                'group_id': self.kafka_settings['group.id'],
                'offset': msg.offset(),
                'partition': msg.partition(),
                'key': msg.key(),
                'payload': hexdump(msg.value(), result='return')
                }

        body = (("Metadata:\n"
                 "Topic:     {m[topic]}\n"
                 "Group ID:  {m[group_id]}\n"
                 "Offset:    {m[offset]}\n"
                 "Partition: {m[partition]}\n"
                 "Key:       {m[key]}\n"
                 "\n"
                 "Message:\n"
                 "{m[payload]}\n"
                 ).format(m=data))

        return {
            'statusCode': 200,
            'headers': {'content-type': 'text/plain'},
            'body': body
        }
