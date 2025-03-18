# common/rabbitmq_client.py
import pika
import logging

class RabbitMQClient:
    def __init__(self, host='localhost', queue_name=None, username='guest', password='guest'):
        self.host = host
        self.queue_name = queue_name
        self.username = username
        self.password = password
        self.connection = None
        self.channel = None
        self.logger = logging.getLogger(__name__)
        self.connect()

    def connect(self):
        try:
            credentials = pika.PlainCredentials(self.username, self.password)
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=self.host, credentials=credentials)
            )
            self.channel = self.connection.channel()
            if self.queue_name:
                self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.logger.info(f"Connected to RabbitMQ on {self.host}, queue: {self.queue_name}")
        except Exception as e:
            self.logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    def publish(self, message):
        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=self.queue_name,
                body=message.encode(),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            self.logger.info(f"Published message to {self.queue_name}: {message}")
        except (pika.exceptions.ChannelClosed, pika.exceptions.ConnectionClosed):
            self.logger.warning("Channel closed, reconnecting...")
            self.connect()
            self.publish(message)  # ניסיון חוזר
        except Exception as e:
            self.logger.error(f"Failed to publish message: {e}")
            raise

    def consume(self, callback):
        try:
            self.channel.basic_qos(prefetch_count=1)
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            self.logger.info(f"Started consuming from {self.queue_name}")
            self.channel.start_consuming()
        except Exception as e:
            self.logger.error(f"Failed to start consuming: {e}")
            raise

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            self.logger.info("RabbitMQ connection closed")