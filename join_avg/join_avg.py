import csv
import json
import os
import time
import signal

from common.middleware import Middleware

JOINERS = int(os.environ["JOINERS"])


class Join:

    def __init__(self):
        self.tmp_file = open('tmp.csv', 'a')
        self.consumer_id = os.environ["JOINER_NUM"]

        self.stored_comments = False
        self.avg = 0
        self.tmp = []
        self.all_comments_received = [False] * JOINERS

        self.middleware = Middleware('rabbitmq')

        join_queue = {
            'exchange': 'avg_join',
            'keys': ["A{}".format(self.consumer_id), "{}".format(self.consumer_id)]
        }

        self.middleware.subscribe(join_queue, self.callback)

        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.tmp_file.close()
        self.middleware.shutdown()

    def proses_stored_comments(self):
        with open('tmp.csv') as file:
            csvreader = csv.reader(file)

            result_queue = {
                'exchange': 'result',
                'key': 'STUD'
            }

            liked_memes = []
            for comment in csvreader:
                if int(comment[1]) > self.avg and comment[0] != "":
                    liked_memes.append(comment[0])
            if len(liked_memes) > 0:
                data = json.dumps(liked_memes).encode()
                self.middleware.publish(result_queue, data)

        if False not in self.all_comments_received:
            print("END")
            data = ["END", str(self.consumer_id)]
            body = json.dumps(data).encode()
            self.middleware.publish(result_queue, body)
            self.middleware.shutdown()

    def callback(self, ch, method, properties, body):
        body = body.decode()

        result_queue = {
            'exchange': 'result',
            'key': 'STUD'
        }

        if method.routing_key == "A{}".format(self.consumer_id):
            self.avg = float(json.loads(body))

            if self.stored_comments:
                self.tmp_file.close()
                self.proses_stored_comments()
                self.stored_comments = False
        else:
            comments = json.loads(body)
            if "END" != comments[0]:
                if self.avg > 0:
                    liked_memes = []
                    for comment in comments:
                        if int(comment[1]) > self.avg and comment[0] != "":
                            liked_memes.append(comment[0])
                    if len(liked_memes) > 0:
                        data = json.dumps(liked_memes).encode()
                        self.middleware.publish(result_queue, data)
                else:
                    self.stored_comments = True
                    write = csv.writer(self.tmp_file)
                    write.writerows(comments)
            else:
                producer_id = int(comments[1])
                self.all_comments_received[producer_id] = True

                print("JOINER {} ENDED".format(producer_id))

                if False not in self.all_comments_received and not self.stored_comments:
                    print("END")
                    data = ["END", str(self.consumer_id)]
                    body = json.dumps(data).encode()
                    self.middleware.publish(result_queue, body)
                    self.middleware.shutdown()

    def start_consumer(self):
        print("Waiting for messages.")
        self.middleware.wait_for_messages()
        self.middleware.close()


time.sleep(20)
join = Join()
join.start_consumer()
