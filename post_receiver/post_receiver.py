#!/usr/bin/env python3

import time
import pika
import pika.exceptions
import json
import signal
import os

ADDERS = int(os.environ["ADDERS"])
DISPATCHERS = int(os.environ["DISPATCHERS"])


def parce_avg(msg):
    tmp = json.loads(msg)

    chunk_id = tmp[0][1]

    result = []

    for post in tmp:
        result.append(post[11])

    return chunk_id, result


def get_id_and_url(msg):
    data = json.loads(msg)

    result = []

    for post in data:
        tmp = [post[1], post[8], post[11]]
        result.append(tmp)

    return result


class Receiver:
    def __init__(self):
        self.stopping = False
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
        self.channel.queue_declare(queue='posts')
        self.channel.basic_consume(
            queue='posts', on_message_callback=self.posts_callback, auto_ack=True)

        self.channel.exchange_declare(exchange='adder_queue', exchange_type='direct')
        self.channel.confirm_delivery()

        self.channel.exchange_declare(exchange='post_join_dispatch', exchange_type='direct')
        self.channel.confirm_delivery()
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.stopping = True

        self.channel.stop_consuming()

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
                print("Message was returned from exchange {} key {}".format(exchange, key))

    def posts_callback(self, ch, method, properties, body):
        msg = body.decode()

        if msg != "END":
            chunk_id, result = parce_avg(msg)
            key = str(hash(chunk_id) % ADDERS)
            self.publish('adder_queue', key, result)

            result = get_id_and_url(msg)
            key = str(hash(chunk_id) % DISPATCHERS)
            self.publish('post_join_dispatch', key, result)
        else:
            for i in range(ADDERS):
                self.channel.basic_publish(
                    exchange='adder_queue',
                    routing_key=str(i),
                    body="END".encode())
            for i in range(DISPATCHERS):
                self.channel.basic_publish(
                    exchange='post_join_dispatch',
                    routing_key="{}".format(i),
                    body="END".encode())
            self.connection.close()

    def receive_posts(self):
        print('Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()
        if self.stopping:
            self.connection.close()


# Wait for rabbitmq
time.sleep(20)
receiver = Receiver()
receiver.receive_posts()
