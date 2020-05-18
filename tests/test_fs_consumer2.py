import asyncio
import threading
import unittest
import time
from multiprocessing.pool import Pool

import httpx
import numpy

from config.test_config import TestConfig
from fs_consumer2 import FSConsumer2, http_post


class MockMessage:

    def __init__(self, topic_name="test"):
        self._topic = topic_name
        self._error = None
        self._timestamp = [0, int(time.time())]

        # payload (integers)
        payload_in_num_of_ints = int((750 * 1000) / 3)
        # print("[MockMessage] - generating new payload of {} ints".format(payload_in_num_of_ints))
        random_ints_ndarray = numpy.random.randint(2 ** 14, (2 ** 20) - 1, size=payload_in_num_of_ints)
        self._random_ints = random_ints_ndarray.tolist()

    def topic(self):
        return self._topic

    def timestamp(self):
        return self._timestamp

    def error(self):
        return self._error

    def value(self):
        return self._random_ints

    def __len__(self, *args, **kwargs):
        return len(self._random_ints)


class MockKafkaConsumer:

    def __init__(self, topic_name="test"):
        print("[MockKafkaConsumer] - topic_name={}".format(topic_name))
        self._topic_name = topic_name

    def poll(self, interval):
        # print("[MockKafkaConsumer] - poll called with topic_name {}".format(self._topic_name))
        message = MockMessage(self._topic_name)
        return message

    def subscribe(self, topic_name):
        print("[MockKafkaConsumer] - Subscribe {}".format(self._topic_name))

    def close(self):
        print("[MockKafkaConsumer] - Close")


class TestFSConsumer2(unittest.TestCase):

    def test_post_no_timeout(self):

        topic_name = "test_topic"
        consumer_id = "a556667"
        mock_kafka_consumer = MockKafkaConsumer(topic_name)
        fs_consumer = FSConsumer2(mock_kafka_consumer, consumer_id, topic_name, TestConfig.config())

        response = asyncio.run(fs_consumer.post("http://localhost:9000/test", "{'test': 'test'}"))

        self.assertEqual(200, response.status_code)

    def test_post_with_error(self):

        topic_name = "test_topic"
        consumer_id = "a556667"
        mock_kafka_consumer = MockKafkaConsumer(topic_name)
        fs_consumer = FSConsumer2(mock_kafka_consumer, consumer_id, topic_name, TestConfig.config())

        try:
            response = asyncio.run(fs_consumer.post("http://localhost:9000/test", "{'test': 'test'}"))
            self.assertTrue(False)
        except httpx.NetworkError as e:
            pass

    def test_post_with_timeout(self):
        pool = Pool(processes=10)
        pool.apply_async(http_post, ("http://localhost:9000/test-with-timeout", "{'test': 'test'}"))

        # thread = threading.Thread(target=fs_consumer.post, args=["http://localhost:9000/test-with-timeout", "{'test': 'test'}"])
        # thread.start()

        time.sleep(10)


    def test_post_no_timeout(self):

        topic_name = "test_topic"
        consumer_id = "a556667"
        mock_kafka_consumer = MockKafkaConsumer(topic_name)
        fs_consumer = FSConsumer2(mock_kafka_consumer, consumer_id, topic_name, TestConfig.config())

        response = None
        try:
            response = asyncio.create_task(fs_consumer.post("http://localhost:9000/test", "{'test': 'test'}"))
        except httpx.ReadTimeout as e:
            print(e)

        self.assertEqual(200, response.status_code)

    def test_fs_consumer2(self):
        topic_name = "test_topic"
        consumer_id = "a556667"
        mock_kafka_consumer = MockKafkaConsumer(topic_name)
        fs_consumer = FSConsumer2(mock_kafka_consumer, consumer_id, topic_name, TestConfig.config())

        self.thread = threading.Thread(target=fs_consumer.run)
        self.thread.start()

        print("Running for 10 seconds...")
        time.sleep(10)

        # interrogate the consumer
        total = fs_consumer.total_kbs()
        print("total {}".format(total))
        self.assertTrue(total > 0)

        # stop the consumer
        fs_consumer.stop()
