#!/usr/bin/env python3

import csv
import json
import time
import signal
import os

from common.middleware import Middleware

NUMBER_OF_ROW = int(os.environ["NUMBER_OF_ROW"])
AVG_JOINER = int(os.environ["AVG_JOINER"])


class Client:

    def __init__(self):
        self.stopping = False
        self.consuming = False

        self.file_name = ""
        self.all_student_memes = [False] * AVG_JOINER
        self.all_received = [False] * 3

        self.middleware = Middleware('rabbitmq')

        result_queue = {
            'exchange': 'result',
            'keys': ["AVG", "SENT", "STUD"]
        }

        self.middleware.subscribe(result_queue, self.callback)

        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.stopping = True

        if self.consuming:
            self.middleware.shutdown()

    def send_posts(self, file_path):
        file = open(file_path)
        csvreader = csv.reader(file)

        post_queue = {
            'queue': 'posts'
        }

        header = []
        header = next(csvreader)

        rows = []

        for row in csvreader:
            rows.append(row)

            if len(rows) >= NUMBER_OF_ROW:
                data = json.dumps(rows).encode()
                self.middleware.publish(post_queue, data)
                rows = []
                if self.stopping:
                    self.middleware.close()
                    file.close()
                    exit(0)

        if len(rows) > 0:
            data = json.dumps(rows).encode()
            self.middleware.publish(post_queue, data)

        self.middleware.publish(post_queue, "END".encode())
        print("END_OF_POSTS")
        file.close()

    def send_comments(self, file_path):
        file = open(file_path)
        csvreader = csv.reader(file)

        comments_queue = {
            'queue': 'comments'
        }

        header = []
        header = next(csvreader)

        rows = []

        for row in csvreader:
            rows.append(row)

            if len(rows) >= NUMBER_OF_ROW:
                data = json.dumps(rows).encode()
                self.middleware.publish(comments_queue, data)
                rows = []
                if self.stopping:
                    self.middleware.close()
                    file.close()
                    exit(0)

        if len(rows) > 0:
            data = json.dumps(rows).encode()
            self.middleware.publish(comments_queue, data)

        self.middleware.publish(comments_queue, "END".encode())
        print("END_OF_COMMENTS")

        file.close()

        self.consuming = True

        self.middleware.wait_for_messages()
        self.middleware.close()

    def callback(self, ch, method, properties, body):
        if method.routing_key == "AVG":
            body = body.decode()
            print("Average post score: {}".format(body))
            self.all_received[0] = True
        elif method.routing_key == "SENT":
            if self.file_name != "":
                file_path = "../CSV/{}".format(self.file_name)

                with open(file_path, 'wb') as file:
                    file.write(body)
                    file.close()

                print("Received max average sentiment image")
                self.all_received[1] = True
            else:
                body = body.decode()
                self.file_name = body
        else:
            body = body.decode()
            urls = json.loads(body)
            if "END" != urls[0]:
                with open('../CSV/student_memes.txt', 'a') as f:
                    for url in urls:
                        f.write(url)
                        f.write("\n")
                    f.close()
            else:
                producer_id = int(urls[1])
                self.all_student_memes[producer_id] = True

                if False not in self.all_student_memes:
                    print("RECEIVED ALL STUDENT MEMES")
                    self.all_received[2] = True

        if False not in self.all_received:
            ch.stop_consuming()


if __name__ == '__main__':
    time.sleep(20)
    client = Client()
    client.send_posts('../CSV/the-reddit-irl-dataset-posts.csv')
    client.send_comments('../CSV/the-reddit-irl-dataset-comments.csv')
