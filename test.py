import sys
import time

from confluent_kafka import Consumer

print('Connecting to Kafka @ {}'.format(kafka_servers))
c = Consumer({
    'bootstrap.servers': kafka_servers,
    'group.id': 'mygroup',
    'auto.offset.reset': read_topic_from
})
