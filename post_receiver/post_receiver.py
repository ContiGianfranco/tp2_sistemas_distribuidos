#!/usr/bin/env python3

import time
import json
import signal
import os

from common.middleware import Middleware

ADDERS = int(os.environ["ADDERS"])
DISPATCHERS = int(os.environ["DISPATCHERS"])


def parce_avg(msg):
    tmp = json.loads(msg)

    chunk_id = tmp[0][1]

    result = []

    for post in tmp:
        result.append(post[11])

    return chunk_id, result


def get_id_and_url(msg):
    data = json.loads(msg)

    result = []

    for post in data:
        tmp = [post[1], post[8], post[11]]
        result.append(tmp)

    return result


class Receiver:
    def __init__(self):
        self.middleware = Middleware('rabbitmq')

        post_queue = {
            'queue': 'posts'
        }

        self.middleware.subscribe(post_queue, self.posts_callback)

        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.middleware.shutdown()

    def posts_callback(self, ch, method, properties, body):
        msg = body.decode()

        if msg != "END":
            chunk_id, result = parce_avg(msg)
            key = str(hash(chunk_id) % ADDERS)
            adder_queue = {
                    'exchange': 'adder_queue',
                    'key': key
                }
            self.middleware.publish(adder_queue, json.dumps(result).encode())

            result = get_id_and_url(msg)
            key = str(hash(chunk_id) % DISPATCHERS)
            join_dispatch_queue = {
                'exchange': 'post_join_dispatch',
                'key': key
            }
            self.middleware.publish(join_dispatch_queue, json.dumps(result).encode())
        else:
            for i in range(ADDERS):
                adder_queue = {
                    'exchange': 'adder_queue',
                    'key': str(i)
                }
                self.middleware.publish(adder_queue, "END".encode())
            for i in range(DISPATCHERS):
                join_dispatch_queue = {
                    'exchange': 'post_join_dispatch',
                    'key': str(i)
                }
                self.middleware.publish(join_dispatch_queue, "END".encode())
            self.middleware.shutdown()

    def receive_posts(self):
        print('Waiting for messages. To exit press CTRL+C')
        self.middleware.wait_for_messages()
        self.middleware.close()


# Wait for rabbitmq
time.sleep(20)
receiver = Receiver()
receiver.receive_posts()
