# data_loader/loader.py
import time
from common.rabbitmq_client import RabbitMQClient
from common.logger import setup_logger
from db_handler import DBHandler


class DataLoader:
    def __init__(self, input_queue, output_queue, db_path):
        self.logger = setup_logger('DataLoader', 'data_loader.log')
        self.input_rabbitmq = RabbitMQClient(host='localhost', queue_name=input_queue)
        self.output_rabbitmq = RabbitMQClient(host='localhost', queue_name=output_queue)
        self.db = DBHandler(db_path)

    def fetch_additional_file(self, original_file):
        additional_file = f"extra_{original_file}"
        self.logger.info(f"Fetching additional file: {additional_file}")
        time.sleep(1)  # סימולציה
        return additional_file

    def process_file(self, ch, method, properties, body):
        file_name = body.decode()
        self.logger.info(f"Received file: {file_name}")

        self.db.load_file(file_name)
        if self.db.check_match(file_name):
            self.logger.info(f"Match found for {file_name}")
            additional_file = self.fetch_additional_file(file_name)
            self.output_rabbitmq.publish(additional_file)
        else:
            self.logger.debug(f"No match for {file_name}")

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        self.logger.info("Starting DataLoader")
        self.input_rabbitmq.consume(self.process_file)