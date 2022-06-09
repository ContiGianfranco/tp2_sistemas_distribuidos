import json
import os
import re
import time
import signal

from common.middleware import Middleware

JOINER = int(os.environ["JOINER"])


def is_from_student(body):

    if re.search('university|college|student|teacher|professor', body, re.IGNORECASE):
        return "T"

    return "F"


class Student:

    def __init__(self):
        self.consumer_id = os.environ["INSTANCE_NUM"]

        self.middleware = Middleware('rabbitmq')

        student_queue = {
            'exchange': 'student_queue',
            'keys': ["{}".format(self.consumer_id)]
        }

        self.middleware.subscribe(student_queue, self.callback)

        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.middleware.shutdown()

    def proces_student(self, comments):
        result = {}

        for i in range(JOINER):
            result[i] = []
        for comment in comments:
            comment_id = comment[0]
            key = hash(comment_id) % JOINER
            tmp = [comment[0], is_from_student(comment[1]), comment[2]]
            result[key].append(tmp)

        return result

    def callback(self, ch, method, properties, body):
        msg = body.decode()

        if msg != "END":
            comments = json.loads(msg)
            shards = self.proces_student(comments)

            for i in range(JOINER):
                if len(shards[i]) > 0:
                    shard = shards[i]
                    key = "C{}".format(i)
                    joiner_queue = {
                        'exchange': 'joiner_queue',
                        'key': key
                    }
                    self.middleware.publish(joiner_queue, json.dumps(shard).encode())
        else:
            for i in range(JOINER):
                data = ["END", str(self.consumer_id)]
                key = "C{}".format(i)
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
dispatch = Student()
dispatch.start_consumer()
