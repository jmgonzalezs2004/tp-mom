import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):

        self.host = host
        self.queue_name = queue_name
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name)
        except Exception as e:
            pass

    def start_consuming(self, on_message_callback):

        try:

            def callback(ch, method, properties, body):
                ack = lambda:ch.basic_ack(delivery_tag = method.delivery_tag)
                nack = lambda:ch.basic_nack(delivery_tag = method.delivery_tag)
                on_message_callback(body, ack, nack)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            self.channel.start_consuming()
        except Exception as e:
            pass
        
    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except Exception as e:
            pass

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='',
                                  routing_key=self.queue_name,
                                  body=message)
        except Exception as e:
            pass

    def close(self):
        try:
            self.connection.close()
        except Exception as e:
            pass
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

