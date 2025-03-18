# file_processor/processor.py
import time
from common.rabbitmq_client import RabbitMQClient
from common.logger import setup_logger


class FileProcessor:
    def __init__(self, queue_name):
        self.logger = setup_logger('FileProcessor', 'file_processor.log')
        self.rabbitmq = RabbitMQClient(host='localhost', queue_name=queue_name)

    def process_file(self, ch, method, properties, body):
        file_name = body.decode()
        self.logger.info(f"Received file: {file_name}")

        self.logger.info(f"Processing {file_name}")
        time.sleep(2)  # סימולציה
        self.logger.info(f"Finished processing {file_name}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self.logger.info("Starting FileProcessor")
        self.rabbitmq.consume(self.process_file)