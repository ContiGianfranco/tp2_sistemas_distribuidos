import csv
import json
import pika
import pika.exceptions
import os
import time
import signal

JOINERS = int(os.environ["JOINERS"])


class Join:

    def __init__(self):
        self.stopping = False

        self.consumer_id = os.environ["JOINER_NUM"]

        self.stored_comments = False
        self.avg = 0
        self.tmp = []
        self.all_comments_received = [False] * JOINERS

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
        self.channel.exchange_declare(exchange='avg_join', exchange_type='direct')

        self.result = self.channel.queue_declare(queue='', durable=True)
        self.queue_name = self.result.method.queue

        self.channel.queue_bind(
            exchange='avg_join', queue=self.queue_name, routing_key="A{}".format(self.consumer_id))
        self.channel.queue_bind(
            exchange='avg_join', queue=self.queue_name, routing_key="{}".format(self.consumer_id))

        self.channel.exchange_declare(exchange='result', exchange_type='direct')
        self.channel.confirm_delivery()

        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.stopping = True

        self.channel.stop_consuming()

    def proses_stored_comments(self):
        file = open('tmp.csv')
        csvreader = csv.reader(file)

        liked_memes = []
        for comment in csvreader:
            if int(comment[1]) > self.avg and comment[0] != "":
                liked_memes.append(comment[0])
        if len(liked_memes) > 0:
            data = json.dumps(liked_memes).encode()
            self.publish('result', 'STUD', data)

        file.close()

        if False not in self.all_comments_received:
            print("END")
            data = ["END", str(self.consumer_id)]
            body = json.dumps(data).encode()
            self.channel.basic_publish(exchange='result', routing_key='STUD', body=body)
            self.connection.close()

    def publish(self, exchange, key, body):
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

    def callback(self, ch, method, properties, body):
        body = body.decode()

        if method.routing_key == "A{}".format(self.consumer_id):
            self.avg = float(json.loads(body))

            if self.stored_comments:
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
                        self.publish('result', 'STUD', data)
                else:
                    self.stored_comments = True
                    with open('tmp.csv', 'a') as f:
                        write = csv.writer(f)
                        write.writerows(comments)
                        f.close()
            else:
                producer_id = int(comments[1])
                self.all_comments_received[producer_id] = True

                print("JOINER {} ENDED".format(producer_id))

                if False not in self.all_comments_received and not self.stored_comments:
                    print("END")
                    data = ["END", str(self.consumer_id)]
                    body = json.dumps(data).encode()
                    self.channel.basic_publish(exchange='result', routing_key='STUD', body=body)
                    self.connection.close()

    def start_consumer(self):
        print("Waiting for messages.")

        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        self.channel.start_consuming()
        if self.stopping:
            self.connection.close()


time.sleep(20)
join = Join()
join.start_consumer()
