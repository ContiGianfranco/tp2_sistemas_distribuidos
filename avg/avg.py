import json
import time
import pika
import pika.exceptions
import signal
import os

ADDERS = int(os.environ["ADDERS"])
AVG_JOINER = int(os.environ["AVG_JOINER"])


class Averager:
    def __init__(self):
        self.stopping = False

        self.count = 0
        self.sum = 0
        self.ended_list = [False for i in range(ADDERS)]

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
        self.channel.queue_declare(queue='avg')
        self.channel.basic_consume(
            queue='avg', on_message_callback=self.callback, auto_ack=True)

        self.channel.exchange_declare(exchange='avg_join', exchange_type='direct')
        self.channel.exchange_declare(exchange='result', exchange_type='direct')
        self.channel.confirm_delivery()
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.stopping = True
        self.channel.stop_consuming()

    def callback(self, ch, method, properties, body):
        print(body.decode())
        msg = json.loads(body.decode())

        self.count += int(msg[1])
        self.sum += int(msg[2])
        self.ended_list[int(msg[0])] = True

        if False not in self.ended_list:
            avg = self.sum / self.count
            print("Average: {}".format(avg))

            for i in range(AVG_JOINER):
                sent = False
                while not sent:
                    try:
                        self.channel.basic_publish(
                            exchange='avg_join',
                            routing_key="A{}".format(i),
                            body=json.dumps(avg).encode(),
                            mandatory=True)
                        sent = True
                    except pika.exceptions.UnroutableError:
                        time.sleep(1)
                        print("Message was returned")

            sent = False
            while not sent:
                try:
                    self.channel.basic_publish(
                        exchange='result',
                        routing_key="AVG",
                        body=str(avg).encode(),
                        mandatory=True)
                    sent = True
                except pika.exceptions.UnroutableError:
                    time.sleep(1)
                    print("Message was returned")

            self.connection.close()

    def start_consumer(self):
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()
        if self.stopping:
            self.connection.close()


# Wait for rabbitmq to come up
time.sleep(20)
averager = Averager()
averager.start_consumer()
