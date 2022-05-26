import json
import pika
import pika.exceptions
import time
import os
import signal

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
        self.stopping = False

        self.consumer_id = os.environ["DISPATCH_NUM"]

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

        self.channel.exchange_declare(exchange='post_join_dispatch', exchange_type='direct')

        self.result = self.channel.queue_declare(queue='', durable=True)
        self.queue_name = self.result.method.queue

        self.channel.queue_bind(
            exchange='post_join_dispatch', queue=self.queue_name, routing_key="{}".format(self.consumer_id))

        self.channel.exchange_declare(exchange='joiner_queue', exchange_type='direct')
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

    def callback(self, ch, method, properties, body):
        msg = body.decode()

        if msg != "END":
            posts = json.loads(msg)
            shards = shard_posts(posts)

            for i in range(JOINER):
                if len(shards[i]) > 0:
                    shard = shards[i]
                    key = "P{}".format(i)
                    self.publish('joiner_queue', key, shard)
        else:
            for i in range(JOINER):
                data = ["END", str(self.consumer_id)]
                key = "P{}".format(i)
                self.publish('joiner_queue', key, data)
            self.connection.close()

    def start_consumer(self):
        print("Waiting for messages.")

        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        self.channel.start_consuming()
        if self.stopping:
            self.connection.close()


time.sleep(20)
dispatch = Dispatch()
dispatch.start_consumer()
