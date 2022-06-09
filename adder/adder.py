#!/usr/bin/env python3
import json
import time
import os
import signal

from common.middleware import Middleware


class Adder:

    def __init__(self):
        self.consumer_id = os.environ["ADDER_NUM"]

        self.score_sum = 0
        self.counter = 0

        self.middleware = Middleware('rabbitmq')

        adder_queue = {
            'exchange': 'adder_queue',
            'keys': [str(self.consumer_id)]
        }

        self.middleware.subscribe(adder_queue, self.callback)
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.middleware.shutdown()

    def callback(self, ch, method, properties, body):
        body = body.decode()
        if body != "END":
            scores = json.loads(body)

            for score in scores:
                self.counter += 1
                self.score_sum += int(score)
        else:
            print("END: [{}, {}, {}]".format(self.consumer_id, self.counter, self.score_sum))
            data = [self.consumer_id, self.counter, self.score_sum]

            avg_queue = {
                'queue': 'avg'
            }

            self.middleware.publish(avg_queue, json.dumps(data).encode())
            self.middleware.shutdown()

    def start_consumer(self):
        print('Waiting for messages.'.format(self.consumer_id))

        self.middleware.wait_for_messages()
        self.middleware.close()


# Wait for rabbitmq to come up
time.sleep(20)
adder = Adder()
adder.start_consumer()
