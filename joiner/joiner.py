import csv
import json
import os
import time
import signal

from common.middleware import Middleware

SENT_ADDER = int(os.environ["SENT_ADDER"])
AVG_JOINER = int(os.environ["AVG_JOINER"])
POST_DISPATCHERS = int(os.environ["POST_DISPATCHERS"])
STUDENT_COMM = int(os.environ["STUDENT_COMM"])
NUMBER_OF_ROW = int(os.environ["NUMBER_OF_ROW"])


class Joiner:

    def __init__(self):
        self.consumer_id = os.environ["JOINER_NUM"]

        self.dic = {}
        self.all_posts_received = [False]*POST_DISPATCHERS
        self.all_comments_received = [False]*STUDENT_COMM
        self.stored_comments = False

        self.middleware = Middleware('rabbitmq')

        joiner_queue = {
            'exchange': 'joiner_queue',
            'keys': ["P{}".format(self.consumer_id), "C{}".format(self.consumer_id)]
        }

        self.middleware.subscribe(joiner_queue, self.callback)

        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.middleware.shutdown()

    def shard_sent_adder(self, comments):
        sent_shard = {}
        avg_shard = {}

        for i in range(SENT_ADDER):
            sent_shard[i] = []
        for i in range(AVG_JOINER):
            avg_shard[i] = []

        for comment in comments:
            sent_id = comment[0]
            avg_id = comment[0]
            sent_key = hash(sent_id) % SENT_ADDER
            avg_key = hash(avg_id) % AVG_JOINER

            if comment[0] in self.dic:
                sent_shard[sent_key].append([comment[0], comment[2], self.dic[comment[0]]["url"]])
                if comment[1] == "T":
                    avg_shard[avg_key].append([self.dic[comment[0]]["url"], self.dic[comment[0]]["score"]])

        return sent_shard, avg_shard

    def proses_stored_comments(self):
        file = open('tmp.csv')
        csvreader = csv.reader(file)

        comments = []

        for comment in csvreader:
            comments.append(comment)

            if len(comments) >= NUMBER_OF_ROW:

                sent_shard, avg_shard = self.shard_sent_adder(comments)

                for i in range(SENT_ADDER):
                    if len(sent_shard[i]) > 0:
                        shard = sent_shard[i]
                        key = str(i)
                        sent_queue = {
                            'exchange': 'sentiment_adder_queue',
                            'key': key
                        }
                        self.middleware.publish(sent_queue, json.dumps(shard).encode())

                for i in range(AVG_JOINER):
                    if len(avg_shard[i]) > 0:
                        shard = avg_shard[i]
                        key = str(i)
                        avg_join_queue = {
                            'exchange': 'avg_join',
                            'key': key
                        }
                        self.middleware.publish(avg_join_queue, json.dumps(shard).encode())

                comments = []

        if len(comments) > 0:
            sent_shard, avg_shard = self.shard_sent_adder(comments)

            for i in range(SENT_ADDER):
                if len(sent_shard[i]) > 0:
                    shard = sent_shard[i]
                    key = str(i)
                    sent_queue = {
                        'exchange': 'sentiment_adder_queue',
                        'key': key
                    }
                    self.middleware.publish(sent_queue, json.dumps(shard).encode())

            for i in range(AVG_JOINER):
                if len(avg_shard[i]) > 0:
                    shard = avg_shard[i]
                    key = str(i)
                    avg_join_queue = {
                        'exchange': 'avg_join',
                        'key': key
                    }
                    self.middleware.publish(avg_join_queue, json.dumps(shard).encode())

        file.close()

    def callback(self, ch, method, properties, body):
        body = body.decode()

        if method.routing_key == "P{}".format(self.consumer_id):
            posts = json.loads(body)
            if posts[0] != "END":
                posts = json.loads(body)

                for post in posts:
                    self.dic[post[0]] = {
                        "url": post[1],
                        "score": post[2]
                    }
            else:
                self.all_posts_received[int(posts[1])] = True
                print("RECEIVED ALL POSTS FROM {}".format(posts[1]))

                if False not in self.all_posts_received:
                    print("RECEIVED ALL POSTS: {}".format(len(self.dic.keys())))
                    if self.stored_comments:
                        self.proses_stored_comments()
                        self.stored_comments = False
                    if False not in self.all_comments_received:
                        for i in range(SENT_ADDER):
                            data = ["END", str(self.consumer_id)]
                            key = str(i)
                            sent_queue = {
                                'exchange': 'sentiment_adder_queue',
                                'key': key
                            }
                            self.middleware.publish(sent_queue, json.dumps(data).encode())
                        for i in range(AVG_JOINER):
                            data = ["END", str(self.consumer_id)]
                            key = str(i)
                            avg_join_queue = {
                                'exchange': 'avg_join',
                                'key': key
                            }
                            self.middleware.publish(avg_join_queue, json.dumps(data).encode())
                        self.middleware.shutdown()
        else:
            comments = json.loads(body)
            if comments[0] == "END":
                self.all_comments_received[int(comments[1])] = True

                if False not in self.all_comments_received:
                    if False not in self.all_posts_received:
                        for i in range(SENT_ADDER):
                            data = ["END", str(self.consumer_id)]
                            key = str(i)
                            sent_queue = {
                                'exchange': 'sentiment_adder_queue',
                                'key': key
                            }
                            self.middleware.publish(sent_queue, json.dumps(data).encode())
                        for i in range(AVG_JOINER):
                            data = ["END", str(self.consumer_id)]
                            key = str(i)
                            avg_join_queue = {
                                'exchange': 'avg_join',
                                'key': key
                            }
                            self.middleware.publish(avg_join_queue, json.dumps(data).encode())
                        self.middleware.shutdown()
            elif False not in self.all_posts_received:
                sent_shard, avg_shard = self.shard_sent_adder(comments)

                for i in range(SENT_ADDER):
                    if len(sent_shard[i]) > 0:
                        shard = sent_shard[i]
                        key = str(i)
                        sent_queue = {
                            'exchange': 'sentiment_adder_queue',
                            'key': key
                        }
                        self.middleware.publish(sent_queue, json.dumps(shard).encode())

                for i in range(AVG_JOINER):
                    if len(avg_shard[i]) > 0:
                        shard = avg_shard[i]
                        key = str(i)
                        avg_join_queue = {
                            'exchange': 'avg_join',
                            'key': key
                        }
                        self.middleware.publish(avg_join_queue, json.dumps(shard).encode())
            else:
                self.stored_comments = True
                comments = json.loads(body)
                with open('tmp.csv', 'a') as f:

                    write = csv.writer(f)
                    write.writerows(comments)

                    f.close()

    def start_consumer(self):
        print("Waiting for messages.")
        self.middleware.wait_for_messages()
        self.middleware.close()


# Wait for rabbitmq to come up
time.sleep(20)
joiner = Joiner()
joiner.start_consumer()
