from confluent_kafka import Producer
from pytest import fixture
import time


class Context(object):
    def __init__(self):
        self.start_time = self.current_milli_time()
        self.end_time = self.start_time + 15_000

    def get_remaining_time_in_millis(self):
        remaining = self.end_time - self.current_milli_time()
        if remaining < 0:
            return 0
        return remaining

    @staticmethod
    def current_milli_time():
        return int(round(time.time() * 1000))


@fixture
def kafka(monkeypatch):
    host = '192.168.99.100:9092'
    monkeypatch.setenv('KAFKA_HOSTS', host)
    return host


@fixture
def kafka_producer(kafka):
    return Producer({
        'bootstrap.servers': kafka,
        'api.version.request': True
    })


@fixture(scope='function')
def context():
    return Context()
