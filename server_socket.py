#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import time
import socket
from multiprocessing import Process

# 75Kb
MESSAGE_SIZE_KB = 75 * 1000

THROUGHPUT_DEBUG_INTERVAL_S = 5

KBS_IN_MB = 1000

socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
address = os.environ["IP_ADDRESS"]
port = 9000
print(f"Binding to address {address}, port {port}")
socket.bind((address, port))
socket.listen(3)


def deliver_bytes(connection, process_id):
    print(f"Process-{process_id}]- spawned for connection {connection}")

    last_subscribe_time = int(time.time())

    nomsg_count = 0

    kbs_so_far = 0

    window_start_time = int(time.time())

    while True:
        current_time = int(time.time())
        # print("time elapsed: {}".format(current_time - last_subscribe_time))

        data = connection.recv(MESSAGE_SIZE_KB)
        msg = bytearray(data)
        # print(f"Received len(msg) {sys.getsizeof(msg)} bytes.")

        if len(msg) is 0:
            # print("No message")
            nomsg_count = nomsg_count + 1
            if 10 < current_time - last_subscribe_time:
                print("number of nomsgs: {}".format(nomsg_count))
                last_subscribe_time = current_time
            continue

        if 10 < current_time - last_subscribe_time:
            print(f"[Process-{process_id}] number of nomsgs: {nomsg_count}")
            nomsg_count = 0
            last_subscribe_time = current_time

        # Maintain figures for throughput reporting
        kbs_so_far += sys.getsizeof(msg) / 1000

        # Determine if we should output a throughput figure
        window_length_sec = current_time - window_start_time

        if window_length_sec >= THROUGHPUT_DEBUG_INTERVAL_S:
            throughput_mb_per_s = float(kbs_so_far / (THROUGHPUT_DEBUG_INTERVAL_S * KBS_IN_MB))
            print(f"Process-{process_id}] - Throughput in window: {throughput_mb_per_s} MB/s")
            # Reset ready for the next throughput indication
            window_start_time = int(time.time())
            kbs_so_far = 0


i = 0
while True:
    connection, address = socket.accept()
    print(f"Connection established with {address}")
    p = Process(target=deliver_bytes, args=(connection, i))
    p.start()
    i += 1

connection.close()
