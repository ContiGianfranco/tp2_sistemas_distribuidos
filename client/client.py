#!/usr/bin/env python3

import csv
import json
import time
import pika
import pika.exceptions
import signal
import os

NUMBER_OF_ROW = int(os.environ["NUMBER_OF_ROW"])
AVG_JOINER = int(os.environ["AVG_JOINER"])


class Client:

    def __init__(self):
        self.stopping = False
        self.consuming = False

        self.file_name = ""
        self.all_student_memes = [False] * AVG_JOINER
        self.all_received = [False] * 3

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

        self.channel.exchange_declare(exchange='result', exchange_type='direct')
        self.result = self.channel.queue_declare(queue='', durable=True)
        self.queue_name = self.result.method.queue

        self.channel.queue_bind(
            exchange='result', queue=self.queue_name, routing_key="AVG")
        self.channel.queue_bind(
            exchange='result', queue=self.queue_name, routing_key="SENT")
        self.channel.queue_bind(
            exchange='result', queue=self.queue_name, routing_key="STUD")

        self.channel.queue_declare(queue='posts')
        self.channel.queue_declare(queue='comments')

        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.stopping = True

        if self.consuming:
            self.channel.stop_consuming()

    def send_posts(self, file_path):
        file = open(file_path)
        csvreader = csv.reader(file)

        header = []
        header = next(csvreader)

        rows = []

        for row in csvreader:
            rows.append(row)

            if len(rows) >= NUMBER_OF_ROW:
                data = json.dumps(rows).encode()
                self.channel.basic_publish(exchange='', routing_key='posts', body=data)
                rows = []
                if self.stopping:
                    self.connection.close()
                    file.close()
                    exit(0)

        if len(rows) > 0:
            data = json.dumps(rows).encode()
            self.channel.basic_publish(exchange='', routing_key='posts', body=data)

        self.channel.basic_publish(exchange='', routing_key='posts', body="END".encode())
        print("END_OF_POSTS")
        file.close()

    def send_comments(self, file_path):
        file = open(file_path)
        csvreader = csv.reader(file)

        header = []
        header = next(csvreader)

        rows = []

        for row in csvreader:
            rows.append(row)

            if len(rows) >= NUMBER_OF_ROW:
                data = json.dumps(rows).encode()
                self.channel.basic_publish(exchange='', routing_key='comments', body=data)
                rows = []
                if self.stopping:
                    self.connection.close()
                    file.close()
                    exit(0)

        if len(rows) > 0:
            data = json.dumps(rows).encode()
            self.channel.basic_publish(exchange='', routing_key='comments', body=data)

        self.channel.basic_publish(exchange='', routing_key='comments', body="END".encode())
        print("END_OF_COMMENTS")

        file.close()

        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)

        self.consuming = True

        self.channel.start_consuming()
        if self.stopping:
            self.connection.close()

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
            self.connection.close()


if __name__ == '__main__':
    time.sleep(20)
    client = Client()
    client.send_posts('../CSV/the-reddit-irl-dataset-posts.csv')
    client.send_comments('../CSV/the-reddit-irl-dataset-comments.csv')
