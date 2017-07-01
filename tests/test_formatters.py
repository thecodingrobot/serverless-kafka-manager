import api.formatter as formatters


class DummyMessage:
    def __init__(self, payload):
        self.payload = payload

    def topic(self):
        return 'topic_1'

    def offset(self):
        return 0

    def partition(self):
        return 1

    def key(self):
        return 'my_key'.encode()

    def value(self):
        if type(self.payload) is str:
            return self.payload.encode()
        else:
            return self.payload


def test_raw_formatter_text_payload():
    settings = {
        'opt1': 1
    }
    f = formatters.RawFormatter(settings)
    result = f.format_message(DummyMessage('HelloWorld'))
    assert result == {
        'statusCode': 200,
        'headers': {'content-type': 'text/plain'},
        'body': 'HelloWorld'
    }


def test_raw_formatter_binary_payload():
    settings = {
        'opt1': 1
    }
    payload = bytes([0x6c, 0x69, 0x62, 0x6c, 0x7a, 0x34, 0x2e, 0x73, 0x6f, 0xa0])

    f = formatters.RawFormatter(settings)
    result = f.format_message(DummyMessage(payload))
    assert result == {
        'statusCode': 200,
        'headers': {'content-type': 'text/plain'},
        'body': 'liblz4.so\udca0'
    }


def test_json_formatter_payload():
    settings = {
        'group.id': 1
    }
    f = formatters.JsonFormatter(settings)

    msg = DummyMessage('HelloWorld')
    result = f.format_message(msg)
    assert result == {
        'statusCode': 200,
        'body': ('{"topic": "topic_1", '
                 '"group_id": 1, '
                 '"offset": 0, '
                 '"partition": 1, '
                 '"key": "my_key", '
                 '"payload": "HelloWorld"}')
    }


def test_hexdump_formatter_payload():
    settings = {
        'group.id': 1
    }

    f = formatters.HexdumpFormatter(settings)

    msg = DummyMessage('HelloWorld')
    result = f.format_message(msg)
    assert result == {
        'statusCode': 200,
        'headers': {'content-type': 'text/plain'},
        'body': ('Metadata:\n'
                 "Topic:     topic_1\n"
                 "Group ID:  1\n"
                 "Offset:    0\n"
                 "Partition: 1\n"
                 "Key:       b'my_key'\n\n"
                 "Message:\n00000000: 48 65 6C 6C 6F 57 6F 72  6C 64                    HelloWorld\n")
    }
