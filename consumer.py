#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import requests
import json

from confluent_kafka import Consumer

# equivalent to: curl endpoint --header "Content-Type: application/json" --request POST --data data endpoint_url
def post(endpoint_url,payload):
  # uses lib requests
  headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
  request = requests.post(endpoint_url, data=json.dumps(payload), headers=headers)

###
### PLEASE SET THE BELOW CONFIGURATION
###

# currently posts to endpoint using requests: https://2.python-requests.org/en/v2.9.1/
endpoint_url="http://focussensors.duckdns.org:900/consumer_reporting_endpoint"
consumer_id=os.environ["POD_NAME"] or "unknown"

# Address of the kafka servers and topic name
#kafka_servers = '192.168.56.101:9092'
kafka_servers = 'internal-service-0.kafka.svc.cluster.local:32400'
topic_name = 'test'

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
    'group.id': 'mygroup',
    'auto.offset.reset': read_topic_from
})

c.subscribe([topic_name])

kbs_so_far = 0

window_start_time = int(time.time())

timestamps = []


post(endpoint_url, dict(
    timestamp = window_start_time,
    message = "consumer initialized",
    id = consumer_id
))

while True:
    
    # Waits 1 second to receive a message, if it doesn't find one goes round the loop again
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    timestamps.append(msg.timestamp()[1])
    
    current_time = int(time.time())
            
    # Maintain figures for throughput reporting
    kbs_so_far += sys.getsizeof(msg.value())/1000
    
    # Determine if we should output a throughput figure
    window_length_sec = current_time - window_start_time
    
    if window_length_sec >= throughput_debug_interval_in_sec:
        throughput_mb_per_s = int(kbs_so_far / (throughput_debug_interval_in_sec*kbs_in_mb))
        # print('Throughput in window: {} MB/s'.format(throughput_mb_per_s))
        report(endpoint_url, current_time, throughput_mb_per_s, timestamps)

        # Reset ready for the next throughput indication
        window_start_time = int(time.time())
        kbs_so_far = 0
        timestamps = []
    
c.close()

def report(endpoint_url, current_time, throughput_mb_per_s, timestamps):
  if endpoint_url.startswith("http://"):
    min_ts = min(timestamps)
    offsets = [t - min_ts for t in timestamps]
    lateness = [abs(offsets[i] - i) for i in range(offsets)]
    payload=dict(
      timestamp = current_time,
      throughput = throughput_mb_per_s,
      min_timestamp = min(timestamps),
      max_lateness = max(lateness),
      id = consumer_id
    )
    post(endpoint_url, payload)
  else:
    print('Throughput in window: {} MB/s'.format(throughput_mb_per_s))

