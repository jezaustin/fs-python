import json
import os
import random
import resource
import sys
import time

import asyncio
import httpx
from confluent_kafka import Consumer
from confluent_kafka.cimpl import KafkaError
from guppy import hpy

from config.base_config import BaseConfig
from stoppable_thread import StoppableThread


class FSConsumer2(StoppableThread):
    THROUGHPUT_DEBUG_INTERVAL_SEC = 5
    KBS_IN_MB = 1000
    POLL_INTERVAL = 1.0
    _total_kbs = 0.0

    def __init__(self, consumer, consumer_id, topic_list=[], config=BaseConfig.config(), *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.config = config
        self.consumer = consumer
        self.consumer_id = consumer_id
        self.topic_list = topic_list
        self.peak_memory_mb = self.get_peak_memory()
        print("[FSConsumer2] - consumer_id={}, topic_list={}, config={}".format(self.consumer_id, self.topic_list,
                                                                                self.config))

    def poll(self):
        message = self.consumer.poll(self.POLL_INTERVAL)

        meta = {}
        if message is None:
            # print("[FSConsumer2] - Message is None.")
            return meta

        # returns None if no error, KafkaError otherwise
        if message.error() is KafkaError:
            print("[FSConsumer2] - KafkaError from client.")
            meta['error'] = message.error()
            return meta

        # extract the necessary meta (and effectively discard the message payload)
        meta['topic'] = message.topic()
        meta['msg_size'] = sys.getsizeof(message.value()) / 1000
        meta['timestamp'] = message.timestamp()[1]

        return meta

    def get_peak_memory(self):
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

    def run(self):
        print("Starting FSConsumer2 with poll interval {}".format(self.POLL_INTERVAL))

        print("Subscribing to topic(s) {}".format(self.topic_list))
        self.consumer.subscribe(self.topic_list)
        last_subscribe_time = int(time.time())

        nomsg_count = 0
        kbs_so_far = 0
        window_start_time = int(time.time())
        timestamps = dict()

        hp = hpy()
        hp.setrelheap()
        self.get_peak_memory()

        while not self.isStopped():
            # Waits 1 second to receive a message, if it doesn't find one goes round the loop again
            message_meta = self.poll()
            current_time = int(time.time())

            # check if message was received
            if not message_meta:
                nomsg_count = nomsg_count + 1
                if (current_time - last_subscribe_time) < 10:
                    print("number of nomsgs: {}".format(nomsg_count))
                    last_subscribe_time = current_time
                continue

            # check if error from consumer
            if 'error' in message_meta:
                print("Consumer error: {}".format(message_meta['error']))
                continue

            if message_meta['topic'] in timestamps:
                timestamps[message_meta['topic']].append(message_meta['timestamp'])
            else:
                timestamps[message_meta['topic']] = [message_meta['timestamp']]

            # Maintain figures for throughput reporting
            kbs_so_far += message_meta['msg_size']
            self._total_kbs += message_meta['msg_size']

            # Determine if we should output a throughput figure
            window_length_sec = current_time - window_start_time

            if window_length_sec >= self.THROUGHPUT_DEBUG_INTERVAL_SEC:
                window_end_time = int(time.time())
                total_time = window_end_time - window_start_time
                throughput_mb_per_s = float(kbs_so_far / (total_time * self.KBS_IN_MB))
                print('Throughput in window: {} MB/s'.format(throughput_mb_per_s))
                self.peak_memory_mb = self.get_peak_memory()
                print('Peak memory use: {} Mb'.format(self.peak_memory_mb))
                # h = hp.heap()
                # by_types = h.bytype
                # by_refs = h.byrcs
                # print("Heap by types {}".format(by_types))
                self.report(current_time, throughput_mb_per_s, timestamps)

                # Reset ready for the next throughput indication
                window_start_time = int(time.time())
                kbs_so_far = 0
                timestamps = dict()

        self.consumer.close()

    def total_kbs(self):
        return self._total_kbs

    async def post(self, endpoint_url, payload):
        headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        async with httpx.AsyncClient() as client:
            response = await client.post(endpoint_url, data=json.dumps(payload), headers=headers)
        return response

    def report(self, current_time, throughput_mb_per_s, timestamps):
        endpoint_url = None
        try:
            endpoint_url = self.config["ENDPOINT_URL"]
        except KeyError:
            print("[Warning] - missing ENDPOINT_URL in config.")
            endpoint_url = "print"

        if endpoint_url.startswith("http://"):
            topics = timestamps.keys()
            min_ts = {
                topic: min(timestamps[topic])
                for topic in topics
            }
            # min_ts = min(timestamps)
            offsets = {
                topic: [t - min_ts[topic] for t in timestamps[topic]]
                for topic in topics
            }
            # offsets = [t - min_ts for t in timestamps]
            lateness = {
                topic: [abs(offsets[topic][i] - i) for i in range(len(offsets[topic]))]
                for topic in topics
            }
            # lateness = [abs(offsets[i] - i) for i in range(len(offsets))]
            max_lateness = [
                max(lateness[topic])
                for topic in topics
            ]
            payload = dict(
                timestamp=current_time,
                throughput=throughput_mb_per_s,
                # min_timestamp = min(timestamps),
                max_lateness=max(max_lateness),
                consumer_id=self.consumer_id,
                peak_memory_mb=self.peak_memory_mb
            )

            # call async to avoid blocking
            try:
                asyncio.run(self.post(endpoint_url, payload))
            except httpx.ReadTimeout as e:
                print(f"Timeout POSTing to endpoint {endpoint_url}, {e}")
        else:
            print('Throughput in window: {} MB/s'.format(throughput_mb_per_s))


if __name__ == '__main__':
    # get the consumer_id from the environment, if present
    consumer_id = None
    try:
        consumer_id = os.environ["POD_NAME"]
    except KeyError:
        # not available in environment, generate an id
        id_alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
        consumer_id = ''.join([random.choice(id_alphabet) for _ in range(6)])

    kafka_servers = "internal-service-0.kafka.svc.cluster.local:32400"

    # Whether to only listen for messages that occurred since the consumer started ('latest'),
    # or to pick up all messages that the consumer has missed ('earliest').
    # Using 'latest' means the consumer must be started before the producer.
    read_topic_from = 'latest'

    # https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    consumer = Consumer({
        'bootstrap.servers': kafka_servers,
        'group.id': consumer_id,
        'auto.offset.reset': read_topic_from,
        # see https://docs.confluent.io/current/installation/configuration/consumer-configs.html
        'metadata.max.age.ms': 5000,
        'max.partition.fetch.bytes': 7500 * 1024,
        # see https://github.com/confluentinc/confluent-kafka-python/issues/759
        # queue a maximum of 100 messages
        'queued.max.messages.kbytes': 75000
    })

    # topic_name = ["sensor{}".format(i) for i in range(9)]
    # subscribe to all topics
    # see https://docs.confluent.io/current/clients/confluent-kafka-python/#pythonclient-consumer
    topic_list = ["^.*"]
    consumer = FSConsumer2(consumer, consumer_id, topic_list)
    consumer.run()
