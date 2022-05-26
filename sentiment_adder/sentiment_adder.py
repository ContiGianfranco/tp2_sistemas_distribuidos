import json
import pika
import pika.exceptions
import os
import time
import signal

JOINERS = int(os.environ["JOINERS"])


class Adder:

    def __init__(self):
        self.stopping = False

        self.consumer_id = os.environ["ADDER_NUM"]

        connected = False
        while not connected:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host='rabbitmq'))
                connected = True
            except pika.exceptions.AMQPConnectionError:
                print("Rabbitmq not connected yet")
                time.sleep(1)

        self.dic = {}
        self.ended_list = [False for i in range(JOINERS)]

        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='sentiment_adder_queue', exchange_type='direct')

        self.result = self.channel.queue_declare(queue='', durable=True)
        self.queue_name = self.result.method.queue

        self.channel.queue_bind(
            exchange='sentiment_adder_queue', queue=self.queue_name, routing_key="{}".format(self.consumer_id))

        self.channel.queue_declare(queue='sentiment_avg')

        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.stopping = True

        self.channel.stop_consuming()

    def get_avgs(self):
        result = []

        for key in self.dic:
            result.append([self.dic[key]["url"], self.dic[key]["sum"] / self.dic[key]["count"]])

        result.append(["END", str(self.consumer_id)])

        return result

    def callback(self, ch, method, properties, body):
        body = body.decode()
        comments = json.loads(body)

        if "END" != comments[0]:
            for comment in comments:
                if comment[1] != "":
                    if comment[0] in self.dic:
                        self.dic[comment[0]]["count"] = self.dic[comment[0]]["count"] + 1
                        self.dic[comment[0]]["sum"] = self.dic[comment[0]]["sum"] + float(comment[1])
                    else:
                        self.dic[comment[0]] = {
                            "count": 1,
                            "sum": float(comment[1]),
                            "url": comment[2]
                        }
        else:
            producer_id = int(comments[1])
            self.ended_list[producer_id] = True

            print("JOINER {} ENDED".format(producer_id))

            if False not in self.ended_list:
                print("END")
                avgs = self.get_avgs()
                data = json.dumps(avgs).encode()
                self.channel.basic_publish(exchange='', routing_key='sentiment_avg', body=data)

                self.connection.close()

    def start_consumer(self):
        print('Waiting for messages.')

        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)

        self.channel.start_consuming()
        if self.stopping:
            self.connection.close()


# Wait for rabbitmq to come up
time.sleep(20)
adder = Adder()
adder.start_consumer()
