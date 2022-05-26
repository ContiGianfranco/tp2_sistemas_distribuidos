#!/usr/bin/env python3
import json
import pika
import pika.exceptions
import time
import os
import signal


class Adder:

    def __init__(self):
        self.stopping = False
        self.consumer_id = os.environ["ADDER_NUM"]

        self.score_sum = 0
        self.counter = 0

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
        self.channel.exchange_declare(exchange='adder_queue', exchange_type='direct')

        self.result = self.channel.queue_declare(queue='', durable=True)
        self.queue_name = self.result.method.queue

        self.channel.queue_bind(
            exchange='adder_queue', queue=self.queue_name, routing_key=str(self.consumer_id))

        self.channel.queue_declare(queue='avg')
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.stopping = True

        self.channel.stop_consuming()

    def callback(self, ch, method, properties, body):
        body = body.decode()
        if body != "END":
            scores = json.loads(body)

            for score in scores:
                self.counter += 1
                self.score_sum += int(score)
        else:
            print("END: [{}, {}, {}]".format(self.consumer_id, self.counter, self.score_sum))
            data = [self.consumer_id, self.counter, self.score_sum]

            self.channel.basic_publish(exchange='', routing_key='avg', body=json.dumps(data).encode())
            self.connection.close()

    def start_consumer(self):
        print('Waiting for messages. To exit press CTRL+C'.format(self.consumer_id))

        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)

        self.channel.start_consuming()
        if self.stopping:
            self.connection.close()


# Wait for rabbitmq to come up
time.sleep(20)
adder = Adder()
adder.start_consumer()
