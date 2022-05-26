import pika
import pika.exceptions
import time
import json
import signal
import os

STUDENT_COMM = int(os.environ["STUDENT_COMM"])


def get_post_id_from_url(url):
    split = url.split("/")

    return split[6]


def filter_columns(msg):
    data = json.loads(msg)

    result = []

    for row in data:
        tmp = [get_post_id_from_url(row[6]), row[7], row[8]]
        result.append(tmp)

    return result


class Receiver:
    def __init__(self):
        connected = False
        self.stopping = False
        while not connected:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='rabbitmq'))
                connected = True
            except pika.exceptions.AMQPConnectionError:
                print("Rabbitmq not connected yet")
                time.sleep(1)

        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='comments')
        self.channel.basic_consume(
            queue='comments', on_message_callback=self.callback, auto_ack=True)

        self.channel.exchange_declare(exchange='student_queue', exchange_type='direct')
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
                print("Message {} was returned from exchange {} key {}".format(body, exchange, key))

    def callback(self, ch, method, properties, body):
        msg = body.decode()

        if msg != "END":
            comments = filter_columns(msg)

            comment_id = comments[0][0]
            key = str(hash(comment_id) % STUDENT_COMM)

            self.publish('student_queue', key, comments)
        else:
            for i in range(STUDENT_COMM):
                self.channel.basic_publish(
                    exchange='student_queue',
                    routing_key="{}".format(i),
                    body="END".encode())
            self.connection.close()

    def receive_comments(self):
        print('Waiting for messages.')
        self.channel.start_consuming()
        if self.stopping:
            self.connection.close()


# Wait for rabbitmq
time.sleep(20)
receiver = Receiver()
receiver.receive_comments()
