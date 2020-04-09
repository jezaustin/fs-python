#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import requests
import json
import resource
import gc
from guppy import hpy

from confluent_kafka import Consumer

hp = hpy()

# equivalent to: curl endpoint --header "Content-Type: application/json" --request POST --data data endpoint_url
def post(endpoint_url,payload):
  # uses lib requests
  headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
  request = requests.post(endpoint_url, data=json.dumps(payload), headers=headers)

def report(endpoint_url, current_time, throughput_mb_per_s, timestamps):
  if endpoint_url.startswith("http://"):
    topics = timestamps.keys()
    min_ts = {
      topic:min(timestamps[topic])
      for topic in topics
    }
    #min_ts = min(timestamps)
    offsets = {
      topic:[t - min_ts[topic] for t in timestamps[topic]]
      for topic in topics
    }
    #offsets = [t - min_ts for t in timestamps]
    lateness = {
      topic: [abs(offsets[topic][i] - i) for i in range(len(offsets[topic]))]
      for topic in topics
    }
    #lateness = [abs(offsets[i] - i) for i in range(len(offsets))]
    max_lateness = [
      max(lateness[topic])
      for topic in topics
    ]
    payload=dict(
      timestamp = current_time,
      throughput = throughput_mb_per_s,
      #min_timestamp = min(timestamps),
      max_lateness = max(max_lateness),
      id = consumer_id
    )
    post(endpoint_url, payload)
  else:
    print('Throughput in window: {} MB/s'.format(throughput_mb_per_s))

###
### PLEASE SET THE BELOW CONFIGURATION
###

# currently posts to endpoint using requests: https://2.python-requests.org/en/v2.9.1/
endpoint_url="http://focussensors.duckdns.org:9000/consumer_reporting_endpoint"
consumer_id=os.environ["POD_NAME"] or "unknown"

# Address of the kafka servers and topic name
#kafka_servers = '192.168.56.101:9092'
kafka_servers = 'internal-service-0.kafka.svc.cluster.local:32400'
#topic_name = 'test'
#topic_name = "^sensor.*"
topic_name = ["sensor{}".format(i) for i in range(50)]

# Whether to only listen for messages that occurred since the consumer started ('latest'),
# or to pick up all messages that the consumer has missed ('earliest').
# Using 'latest' means the consumer must be started before the producer.
read_topic_from = 'latest'

# How often to indicate data rate in seconds
throughput_debug_interval_in_sec = 5

###
### Consumer code
###

kbs_in_mb = 1000

print('Connecting to Kafka @ {}'.format(kafka_servers))
c = Consumer({
    'bootstrap.servers': kafka_servers,
    'group.id': consumer_id,#'mygroup',
    'auto.offset.reset': read_topic_from,
    #'metadata.max.age.ms': 5000,
    'max.partition.fetch.bytes': 7500 * 1024
})

last_subscribe_time = int(time.time())
c.subscribe(topic_name)
nomsg_count = 0

kbs_so_far = 0

window_start_time = int(time.time())

timestamps = dict()

print(f"endpoint={endpoint_url}")
#print("outside loop")
hp.setrelheap()
h = hp.heap()
while True:
    #print("inside loop")
    # Waits 1 second to receive a message, if it doesn't find one goes round the loop again
    msg = c.poll(1.0)
    current_time = int(time.time())
    #print("time elapsed: {}".format(current_time - last_subscribe_time))

    if msg is None:
        #print("No message")
        nomsg_count = nomsg_count + 1
        if 10 < current_time - last_subscribe_time:
            #c.subscribe([topic_name])
            print("number of nomsgs: {}".format(nomsg_count))
            last_subscribe_time = current_time
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    if msg.topic in timestamps:
        timestamps[msg.topic].append(msg.timestamp()[1])
    else:
        timestamps[msg.topic] = [msg.timestamp()[1]]

    if 10 < current_time - last_subscribe_time:
        #c.subscribe([topic_name])
        print("number of nomsgs: {}".format(nomsg_count))
        nomsg_count = 0
        last_subscribe_time = current_time

    # Maintain figures for throughput reporting
    kbs_so_far += sys.getsizeof(msg.value())/1000

    # Determine if we should output a throughput figure
    window_length_sec = current_time - window_start_time

    if window_length_sec >= throughput_debug_interval_in_sec:
        throughput_mb_per_s = float(kbs_so_far / (throughput_debug_interval_in_sec*kbs_in_mb))
        print('Throughput in window: {} MB/s'.format(throughput_mb_per_s))
        print('Peak memory use: {} Mb'.format(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024))
        by_refs = h.byrcs
        print("Heap by size {}".format(by_refs[0].bysize))
        report(endpoint_url, current_time, throughput_mb_per_s, timestamps)

        # Reset ready for the next throughput indication
        window_start_time = int(time.time())
        kbs_so_far = 0
        timestamps = dict()

    # explicitly delete the message
    #del msg
    #gc.collect()
 
c.close()
