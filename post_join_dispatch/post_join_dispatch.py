import json
import time
import os
import signal

from common.middleware import Middleware

JOINER = int(os.environ["JOINER"])


def shard_posts(posts):
    result = {}

    for i in range(JOINER):
        result[i] = []
    for post in posts:
        post_id = post[0]
        key = hash(post_id) % JOINER
        result[key].append(post)

    return result


class Dispatch:

    def __init__(self):
        self.consumer_id = os.environ["DISPATCH_NUM"]

        self.middleware = Middleware('rabbitmq')

        post_join_dispatch = {
            'exchange': 'post_join_dispatch',
            'keys': ["{}".format(self.consumer_id)]
        }

        self.middleware.subscribe(post_join_dispatch, self.callback)
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.middleware.shutdown()

    def pub(self, exchange, key, data):
        body = json.dumps(data).encode()

    def callback(self, ch, method, properties, body):
        msg = body.decode()

        if msg != "END":
            posts = json.loads(msg)
            shards = shard_posts(posts)

            for i in range(JOINER):
                if len(shards[i]) > 0:
                    shard = shards[i]
                    key = "P{}".format(i)
                    joiner_queue = {
                        'exchange': 'joiner_queue',
                        'key': key
                    }
                    self.middleware.publish(joiner_queue, json.dumps(shard).encode())
        else:
            for i in range(JOINER):
                data = ["END", str(self.consumer_id)]
                key = "P{}".format(i)
                joiner_queue = {
                    'exchange': 'joiner_queue',
                    'key': key
                }
                self.middleware.publish(joiner_queue, json.dumps(data).encode())
            self.middleware.shutdown()

    def start_consumer(self):
        print("Waiting for messages.")
        self.middleware.wait_for_messages()
        self.middleware.close()


time.sleep(20)
dispatch = Dispatch()
dispatch.start_consumer()
