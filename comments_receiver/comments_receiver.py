import time
import json
import signal
import os

from common.middleware import Middleware

STUDENT_COMM = int(os.environ["STUDENT_COMM"])


def get_post_id_from_url(url):
    split = url.split("/")

    return split[6]


def filter_columns(msg):
    data = json.loads(msg)

    result = []

    for row in data:
        tmp = [get_post_id_from_url(row[6]), row[7], row[8]]
        result.append(tmp)

    return result


class Receiver:
    def __init__(self):
        self.middleware = Middleware('rabbitmq')
        comments_queue = {
            'queue': 'comments'
        }
        self.middleware.subscribe(comments_queue, self.callback)
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.middleware.shutdown()

    def callback(self, ch, method, properties, body):
        msg = body.decode()

        if msg != "END":
            comments = filter_columns(msg)
            comment_id = comments[0][0]
            key = str(hash(comment_id) % STUDENT_COMM)
            queue = {
                'exchange': 'student_queue',
                'key': key
            }
            self.middleware.publish(queue, json.dumps(comments).encode())
        else:
            for i in range(STUDENT_COMM):
                queue = {
                    'exchange': 'student_queue',
                    'key': "{}".format(i)
                }
                self.middleware.publish(queue, "END".encode())
            self.middleware.shutdown()

    def receive_comments(self):
        print('Waiting for messages.')
        self.middleware.wait_for_messages()
        self.middleware.close()


# Wait for rabbitmq
time.sleep(20)
receiver = Receiver()
receiver.receive_comments()
