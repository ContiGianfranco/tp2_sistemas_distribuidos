import pika
import pika.exceptions
import time


class Middleware:
    def __init__(self, mom_host):
        self.active_queues = {}

        connected = False
        while not connected:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=mom_host))
                connected = True
            except pika.exceptions.AMQPConnectionError:
                print("Rabbitmq not connected yet")
                time.sleep(1)
        self.channel = self.connection.channel()
        self.channel.confirm_delivery()

    def publish(self, queue, message):
        exchange = ''
        if "exchange" not in queue:
            key = queue['queue']

            if queue['queue'] not in self.active_queues:
                self.channel.queue_declare(queue=queue['queue'])

                self.active_queues[queue['queue']] = {
                    'queue_name': queue['queue']
                }
        else:
            key = queue['key']
            exchange = queue['exchange']

            if queue['exchange'] not in self.active_queues:
                self.channel.exchange_declare(exchange=queue['exchange'], exchange_type='direct')
                result = self.channel.queue_declare(queue='', durable=True)
                queue_name = result.method.queue

                self.active_queues[queue['exchange']] = {
                    'queue_name': queue_name
                }

        sent = False
        while not sent:
            try:
                self.channel.basic_publish(
                    exchange=exchange,
                    routing_key=key,
                    body=message,
                    mandatory=True)
                sent = True
            except pika.exceptions.UnroutableError:
                time.sleep(1)
                print("Message was returned from queue {}".format(queue))

    def subscribe(self, queue, callback_function):
        exchange = ''
        if "exchange" not in queue:
            key = queue['queue']
            queue_name = queue['queue']

            if queue['queue'] not in self.active_queues:
                self.channel.queue_declare(queue=queue['queue'])

                self.active_queues[queue['queue']] = {
                    'queue_name': queue['queue']
                }
        else:
            exchange = queue['exchange']

            if queue['exchange'] not in self.active_queues:
                self.channel.exchange_declare(exchange=queue['exchange'], exchange_type='direct')
                result = self.channel.queue_declare(queue='', durable=True)
                queue_name = result.method.queue

                self.active_queues[queue['exchange']] = {
                    'queue_name': queue_name
                }
            else:
                queue_name = self.active_queues[queue['exchange']]['queue_name']

            for key in queue['keys']:
                self.channel.queue_bind(
                    exchange=exchange, queue=queue_name, routing_key=key)

        self.channel.basic_consume(
            queue=queue_name, on_message_callback=callback_function, auto_ack=True)

    def wait_for_messages(self):
        self.channel.start_consuming()

    def shutdown(self):
        self.channel.start_consuming()

    def close(self):
        self.connection.close()
