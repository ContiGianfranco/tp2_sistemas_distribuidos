import json
import pika
import pika.exceptions
import requests
import os
import time
import signal

SENT_ADDERS = int(os.environ["SENT_ADDERS"])


class Top_sentiment:

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

        self.ended_list = [False for i in range(SENT_ADDERS)]
        self.top_url = ""
        self.top_avg_sent = -2

        self.channel.queue_declare(queue='sentiment_avg')
        self.channel.basic_consume(
            queue='sentiment_avg', on_message_callback=self.callback, auto_ack=True)

        self.channel.exchange_declare(exchange='result', exchange_type='direct')
        self.channel.confirm_delivery()

        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.stopping = True

        self.channel.stop_consuming()

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
        post_avgs = json.loads(body)

        for avg in post_avgs:
            if avg[0] == "END":
                self.ended_list[int(avg[1])] = True
            elif float(avg[1]) > self.top_avg_sent:
                self.top_avg_sent = float(avg[1])
                self.top_url = avg[0]

        if False not in self.ended_list:
            print("MAX AVG SENTIMENT {} FROM {}".format(self.top_avg_sent, self.top_url))
            img_data = requests.get(self.top_url).content
            image_name = self.top_url.split("/")[-1]

            self.publish('result', "SENT", image_name.encode())
            self.publish('result', "SENT", img_data)

            self.connection.close()

    def start_consumer(self):
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()
        if self.stopping:
            self.connection.close()


time.sleep(20)
top = Top_sentiment()
top.start_consumer()
