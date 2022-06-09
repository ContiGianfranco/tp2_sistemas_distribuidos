import json
import os
import time
import signal

from common.middleware import Middleware

JOINERS = int(os.environ["JOINERS"])


class Adder:

    def __init__(self):
        self.consumer_id = os.environ["ADDER_NUM"]

        self.dic = {}
        self.ended_list = [False for i in range(JOINERS)]

        self.middleware = Middleware('rabbitmq')

        sentiment_queue = {
            'exchange': 'sentiment_adder_queue',
            'keys': ["{}".format(self.consumer_id)]
        }

        self.middleware.subscribe(sentiment_queue, self.callback)

        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.middleware.shutdown()

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
                sent_queue = {
                    'queue': 'sentiment_avg'
                }
                self.middleware.publish(sent_queue, data)

                self.middleware.shutdown()

    def start_consumer(self):
        print('Waiting for messages.')
        self.middleware.wait_for_messages()
        self.middleware.close()


# Wait for rabbitmq to come up
time.sleep(20)
adder = Adder()
adder.start_consumer()
