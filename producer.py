#!/home/nicholas/workspace/fs-kafka python3

##!/usr/bin/env python3
# -*- coding: utf-8 -*-

import io, random
import os

from confluent_kafka import Producer
import avro.schema
import avro.io
import time

###
### PLEASE SET THE BELOW CONFIGURATION
###

# ID of this sensor, used to determine what partition to write to
sensor_id = 0

# Approximate size of message payload required to be sent in KB
payload_size_in_kb = os.environ["MESSAGE_SIZE_KB"] or 75

# Total data to send in KB, will determine how long the producer runs for
total_data_to_send_in_kb = 750000

# Upper limit on amount of data that should be sent per time interval (in KB/s)
upper_data_rate_limit_kbs = 75000 

# How often to indicate data rate in seconds
throughput_debug_interval_in_sec = 1

# The kafka producer produce method is async so can get way ahead of writes actually being ack'd by Kafka
# If we don't do this then we can't manage the data rate coming out of this process effectively.
# E.g. if this value is too large then the throughput per second will be bigger than that specified 
# in the variable upper_data_rate_limit_kbs above.
max_payloads_before_flush = 5

# Address of the kafka servers and topic name
#kafka_servers = '192.168.56.101:9092'
kafka_servers = 'internal-service-0.kafka.svc.cluster.local:32400,internal-service-1.kafka.svc.cluster.local:32401,internal-service-2.kafka.svc.cluster.local:32402'
topic_name = 'test'


###
### Do not change the below, use the configuration to calculate some settings
###

# Calculate how many integers in each payload, assuming 3 bytes per int
payload_in_num_of_ints = int((payload_size_in_kb*1000) / 3)

# Calculate payloads to send
payloads_to_send = int(total_data_to_send_in_kb / payload_size_in_kb)

# To store total number of payloads sent
total_payloads_sent = 0

kbs_in_mb = 1000

###
### Setup the Kafka producer and load the message schema
###
print('Connecting to Kafka @ {}' .format(kafka_servers))

p = Producer({'bootstrap.servers': kafka_servers})

# Read the schema for the sensor readings, each message contains an array of readings
schema_path="readings.avsc"
schema = avro.schema.Parse(open(schema_path).read())


###
### Reporting to periodically output rough MB/sec rate.
###
window_start_time = None    # Set later
messages_sent_current_window = 0

rate_current_second = None         # Set later, curret second.  Used to check data rate doesn't exceed upper limit
rate_for_second_so_far = 0         # Ensure we don't exceed the data rate in any given second
rate_exceeded = False              # If this flag is set prevents any further writes so we don't exceed upper limit

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        global window_start_time
        global messages_sent_current_window
        global rate_current_second
        global rate_for_second_so_far
        global rate_exceeded
        global total_payloads_sent
        
        total_payloads_sent += 1
        
        current_time = int(time.time())
        print('Recieved message @ '.format(current_time))        

        if current_time != rate_current_second:
            # We are in a new second, we can reset rate throttling
            rate_exceeded = False
            rate_for_second_so_far = 0 
            rate_current_second = current_time
        
        # Add the payload we've sent to the total so far
        rate_for_second_so_far += payload_size_in_kb
        
        # Check if we've exceeded the upper limit of the rate
        if rate_for_second_so_far >= upper_data_rate_limit_kbs:
            rate_exceeded = True
        
        # Output any throughput debug
        messages_sent_current_window += 1
        
        window_length_sec = current_time - window_start_time

        if window_length_sec >= throughput_debug_interval_in_sec:
            print('Throughput in window: {} MB/s'.format(
                    int((messages_sent_current_window * payload_size_in_kb) / (throughput_debug_interval_in_sec*kbs_in_mb))))
            
            # Reset ready for the next throughput indication
            window_start_time = int(time.time())
            messages_sent_current_window = 0


###
### Pre-calculate payloads to send
###
### Note the random integers to send are pre-generated and serialised to bytes
### Doing the steps in the for loop will significantly slow down the generator
### I.e. pre-generate and serialise to binary everything you want to send before
### beginning the process of repeatedly writing to Kafka
###

# Generate some random integers to send
# Avro uses a variable length integer encoding, pick ints that require a 3 byte encoding
# Avro does this by using one byte of the 8 to indicate if more bytes for the int follow
# Code below ensures 3 bytes are always used to encode an int.
# See Vint section on http://lucene.apache.org/core/3_5_0/fileformats.html#VInt
# Note upper bound is (2**20)-1 and not (2**21)-1 as we are using the signed rather than unsigned integer avro type.
random_ints = [random.randint(2**14, (2**20)-1) for i in range(payload_in_num_of_ints)]

writer = avro.io.DatumWriter(schema)
bytes_writer = io.BytesIO()
encoder = avro.io.BinaryEncoder(bytes_writer)
writer.write({"readings": random_ints}, encoder)
payload = bytes_writer.getvalue()

###
### Start sending the payloads
###


start_time = int(time.time())
window_start_time = int(time.time())
rate_current_second = int(time.time())

for i in range(payloads_to_send):
            
    # Trigger any available delivery report callbacks from previous produce() calls
    # Required as produce is asynchronous, polling tells us when a message has been sent
    p.poll(0)
    
    # Check for rate being exceeded, i.e. we are sending data too quickly
    while rate_exceeded:
        
        # Sleep for 10 msec
        time.sleep(0.01)
        
        # Check the current time
        current_time = int(time.time())
        
        # Remove rate limiting if we are in a new second
        if current_time != rate_current_second:
            rate_exceeded = False
            rate_for_second_so_far = 0

    # Asynchronously produce a message, the delivery report callback
    # will be triggered from poll() above, or flush() below, when the message has
    # been successfully delivered or failed permanently.
    p.produce(topic=topic_name,
              key=None,             # Could remove partition property and specify key to determine partition
              value=payload,
              partition=sensor_id,  # Hard codes the partition to write to
              callback=delivery_report,
              timestamp=start_time - payloads_to_send + i) # emulate one msg per second up to start_time
    
    # Flush periodically so we can manage data rates effectively.
    # Produce is async so can get way ahead of writes actually being ack'd by Kafka.  If we don't do this
    # then we can't manage the data rate effectively.
    # The flush rate may need tuning.
    if (i+1) % max_payloads_before_flush == 0:
        p.flush()

# Wait for any outstanding messages to be delivered and delivery report
# callbacks to be triggered.
p.flush()

end_time = int(time.time())

print('Total payloads sent: {}'.format(total_payloads_sent))
print('Total data sent: {} MB'.format(int((total_payloads_sent*payload_size_in_kb)/1000)))
print('Total time taken: {} seconds'.format(end_time - start_time))
