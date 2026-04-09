import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):

        self.host = host
        self.queue_name = queue_name

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True, arguments={'x-queue-type': 'quorum'})

    def start_consuming(self, on_message_callback):
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=on_message_callback)

    def stop_consuming(self):
        self.channel.stop_consuming()

    def send(self, message):
        self.channel.basic_publish(exchange='',
                              routing_key=self.queue_name,
                              body=message)

    def close(self):
        self.connection.close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        pass

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass

    def send(self, message):
        pass

    def close(self):
        pass

