import json
import time
import signal
import os

from common.middleware import Middleware

ADDERS = int(os.environ["ADDERS"])
AVG_JOINER = int(os.environ["AVG_JOINER"])


class Averager:
    def __init__(self):

        self.count = 0
        self.sum = 0
        self.ended_list = [False for i in range(ADDERS)]

        self.middleware = Middleware('rabbitmq')

        avg_queue = {
            'queue': 'avg'
        }

        self.middleware.subscribe(avg_queue, self.callback)
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.middleware.shutdown()

    def callback(self, ch, method, properties, body):
        print(body.decode())
        msg = json.loads(body.decode())

        self.count += int(msg[1])
        self.sum += int(msg[2])
        self.ended_list[int(msg[0])] = True

        if False not in self.ended_list:
            avg = self.sum / self.count
            print("Average: {}".format(avg))

            for i in range(AVG_JOINER):
                avg_join_queue = {
                    'exchange': 'avg_join',
                    'key': "A{}".format(i)
                }
                self.middleware.publish(avg_join_queue, json.dumps(avg).encode())

            result_queue = {
                'exchange': 'result',
                'key': "AVG"
            }

            self.middleware.publish(result_queue, str(avg).encode())
            self.middleware.shutdown()

    def start_consumer(self):
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.middleware.wait_for_messages()
        self.middleware.close()


# Wait for rabbitmq to come up
time.sleep(20)
averager = Averager()
averager.start_consumer()
