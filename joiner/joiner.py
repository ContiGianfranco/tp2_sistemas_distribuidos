import csv
import json
import pika
import pika.exceptions
import os
import time
import signal

SENT_ADDER = int(os.environ["SENT_ADDER"])
AVG_JOINER = int(os.environ["AVG_JOINER"])
POST_DISPATCHERS = int(os.environ["POST_DISPATCHERS"])
STUDENT_COMM = int(os.environ["STUDENT_COMM"])
NUMBER_OF_ROW = int(os.environ["NUMBER_OF_ROW"])


class Joiner:

    def __init__(self):
        self.stopping = False

        self.consumer_id = os.environ["JOINER_NUM"]

        self.dic = {}
        self.all_posts_received = [False]*POST_DISPATCHERS
        self.all_comments_received = [False]*STUDENT_COMM
        self.stored_comments = False

        connected = False
        while not connected:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='rabbitmq'))
                connected = True
            except pika.exceptions.AMQPConnectionError:
                print("Rabbitmq not connected yet")
                time.sleep(1)

        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='joiner_queue', exchange_type='direct')

        self.result = self.channel.queue_declare(queue='', durable=True)
        self.queue_name = self.result.method.queue

        self.channel.queue_bind(
            exchange='joiner_queue', queue=self.queue_name, routing_key="P{}".format(self.consumer_id))
        self.channel.queue_bind(
            exchange='joiner_queue', queue=self.queue_name, routing_key="C{}".format(self.consumer_id))

        self.channel.exchange_declare(exchange='sentiment_adder_queue', exchange_type='direct')
        self.channel.exchange_declare(exchange='avg_join', exchange_type='direct')
        self.channel.confirm_delivery()
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.stopping = True

        self.channel.stop_consuming()

    def shard_sent_adder(self, comments):
        sent_shard = {}
        avg_shard = {}

        for i in range(SENT_ADDER):
            sent_shard[i] = []
        for i in range(AVG_JOINER):
            avg_shard[i] = []

        for comment in comments:
            sent_key = hash(comment[0]) % SENT_ADDER
            avg_key = hash(comment[0]) % AVG_JOINER

            if comment[0] in self.dic:
                sent_shard[sent_key].append([comment[0], comment[2], self.dic[comment[0]]["url"]])
                if comment[1] == "True":
                    avg_shard[avg_key].append([self.dic[comment[0]]["url"], self.dic[comment[0]]["score"]])

        return sent_shard, avg_shard

    def publish(self, exchange, key, data):
        body = json.dumps(data).encode()
        sent = False
        while not sent:
            try:
                self.channel.basic_publish(
                    exchange=exchange,
                    routing_key=key,
                    body=body,
                    mandatory=True)
                sent = True
            except pika.exceptions.UnroutableError:
                time.sleep(1)
                print("Message {} was returned from exchange {} key {}".format(body, exchange, key))

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
                        self.publish('sentiment_adder_queue', key, shard)

                for i in range(AVG_JOINER):
                    if len(avg_shard[i]) > 0:
                        shard = avg_shard[i]
                        key = str(i)
                        self.publish('avg_join', key, shard)

                comments = []

        if len(comments) > 0:
            sent_shard, avg_shard = self.shard_sent_adder(comments)

            for i in range(SENT_ADDER):
                if len(sent_shard[i]) > 0:
                    shard = sent_shard[i]
                    key = str(i)
                    self.publish('sentiment_adder_queue', key, shard)

            for i in range(AVG_JOINER):
                if len(avg_shard[i]) > 0:
                    shard = avg_shard[i]
                    key = str(i)
                    self.publish('avg_join', key, shard)

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

                            self.publish('sentiment_adder_queue', key, data)
                        for i in range(AVG_JOINER):
                            data = ["END", str(self.consumer_id)]
                            key = str(i)
                            self.publish('avg_join', key, data)
                        self.connection.close()
        else:
            comments = json.loads(body)
            if comments[0] == "END":
                self.all_comments_received[int(comments[1])] = True

                if False not in self.all_comments_received:
                    if False not in self.all_posts_received:
                        for i in range(SENT_ADDER):
                            data = ["END", str(self.consumer_id)]
                            key = str(i)
                            self.publish('sentiment_adder_queue', key, data)
                        for i in range(AVG_JOINER):
                            data = ["END", str(self.consumer_id)]
                            key = str(i)
                            self.publish('avg_join', key, data)
                        self.connection.close()
            elif False not in self.all_posts_received:
                comments = json.loads(body)

                sent_shard, avg_shard = self.shard_sent_adder(comments)

                for i in range(SENT_ADDER):
                    if len(sent_shard[i]) > 0:
                        shard = sent_shard[i]
                        key = str(i)
                        self.publish('sentiment_adder_queue', key, shard)

                for i in range(AVG_JOINER):
                    if len(avg_shard[i]) > 0:
                        shard = avg_shard[i]
                        key = str(i)
                        self.publish('avg_join', key, shard)

            else:
                self.stored_comments = True
                comments = json.loads(body)
                with open('tmp.csv', 'a') as f:

                    write = csv.writer(f)
                    write.writerows(comments)

                    f.close()

    def start_consumer(self):
        print("Waiting for messages.")

        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        self.channel.start_consuming()
        if self.stopping:
            self.connection.close()



# Wait for rabbitmq to come up
time.sleep(20)
joiner = Joiner()
joiner.start_consumer()
