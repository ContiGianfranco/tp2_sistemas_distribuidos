import json
import requests
import os
import time
import signal

from common.middleware import Middleware

SENT_ADDERS = int(os.environ["SENT_ADDERS"])


class Top_sentiment:

    def __init__(self):
        self.middleware = Middleware('rabbitmq')

        self.ended_list = [False for i in range(SENT_ADDERS)]
        self.top_url = ""
        self.top_avg_sent = -2

        sent_queue = {
            'queue': 'sentiment_avg'
        }

        self.middleware.subscribe(sent_queue, self.callback)

        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, sig, frame):
        print("Stopping")
        self.middleware.shutdown()

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

            result_queue = {
                'exchange': 'result',
                'key': "SENT"
            }

            self.middleware.publish(result_queue, image_name.encode())
            self.middleware.publish(result_queue, img_data)

            self.middleware.shutdown()

    def start_consumer(self):
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.middleware.wait_for_messages()
        self.middleware.close()


time.sleep(20)
top = Top_sentiment()
top.start_consumer()
