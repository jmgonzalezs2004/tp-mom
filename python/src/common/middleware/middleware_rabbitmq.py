"""
Middleware RabbitMQ Implementation

Provee las clases necesarias para conectar la interfaz del Middleware
(colas y exchanges) contra un broker RabbitMQ usando la librería pika.
"""

import pika
from pika.exceptions import AMQPError

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    """
    Implementación concreta de una cola (Work Queue) en RabbitMQ.
    """

    def __init__(self, host, queue_name):

        self.host = host
        self.queue_name = queue_name
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(self.host)
            )
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name)
        except AMQPError as e:
            raise MessageMiddlewareDisconnectedError(e) from e

    def start_consuming(self, on_message_callback):
        try:

            # pylint: disable=unused-argument
            def callback(ch, method, properties, body):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                on_message_callback(body, ack, nack)

            self.channel.basic_consume(
                queue=self.queue_name, on_message_callback=callback
            )
            self.channel.start_consuming()
        except AMQPError as e:
            raise MessageMiddlewareDisconnectedError(e) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(e) from e

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except AMQPError as e:
            raise MessageMiddlewareDisconnectedError(e) from e

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange="", routing_key=self.queue_name, body=message
            )
        except AMQPError as e:
            raise MessageMiddlewareDisconnectedError(e) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(e) from e

    def close(self):
        try:
            self.connection.close()
        except AMQPError as e:
            raise MessageMiddlewareCloseError(e) from e

    ## No lo pedia, pero me parecio buena idea agregar close al destructor
    ## para asegurarse de flushear los mensajes y no tener comportamiento inesperado
    def __del__(self):
        try:
            self.close()
        # pylint: disable=broad-exception-caught
        except Exception:
            pass


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    """
    Implementación concreta de un Exchange (tipo direct) en RabbitMQ.
    """
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(self.host)
            )
            self.channel = self.connection.channel()

            # De manera predeterminada usa Exchange Directo, tambien existe topic, headers y fanout.
            # Los test requieren de tipo directo, para poder segmentar por routing_key
            self.channel.exchange_declare(
                exchange=self.exchange_name, exchange_type="direct"
            )
        except AMQPError as e:
            raise MessageMiddlewareDisconnectedError(e) from e

    def start_consuming(self, on_message_callback):
        try:
            result = self.channel.queue_declare(queue="", exclusive=True)
            queue_name = result.method.queue

            for routing in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name, queue=queue_name, routing_key=routing
                )

            # pylint: disable=unused-argument
            def callback(ch, method, properties, body):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                on_message_callback(body, ack, nack)

            self.channel.basic_consume(queue=queue_name, on_message_callback=callback)
            self.channel.start_consuming()
        except AMQPError as e:
            raise MessageMiddlewareDisconnectedError(e) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(e) from e

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except AMQPError as e:
            raise MessageMiddlewareDisconnectedError(e) from e

    def send(self, message):
        try:
            for routing in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name, routing_key=routing, body=message
                )
        except AMQPError as e:
            raise MessageMiddlewareDisconnectedError(e) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(e) from e

    def close(self):
        try:
            self.connection.close()
        except AMQPError as e:
            raise MessageMiddlewareCloseError(e) from e

    ## No lo pedia, pero me parecio buena idea agregar close al destructor
    ## para asegurarse de flushear los mensajes y no tener comportamiento inesperado
    def __del__(self):
        try:
            self.close()
        # pylint: disable=broad-exception-caught
        except Exception:
            pass
