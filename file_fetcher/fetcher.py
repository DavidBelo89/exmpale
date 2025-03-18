# file_fetcher/fetcher.py
import time
import os
from common.rabbitmq_client import RabbitMQClient
from common.logger import setup_logger


class FileFetcher:
    def __init__(self, queue_name):
        self.logger = setup_logger('FileFetcher', 'file_fetcher.log')
        self.rabbitmq = RabbitMQClient(queue_name=queue_name)
        self.last_fetched_file = self._load_last_fetched()

    def _load_last_fetched(self):
        if os.path.exists('last_fetched.txt'):
            with open('last_fetched.txt', 'r') as f:
                return int(f.read().strip())
        return 0

    def _save_last_fetched(self, timestamp):
        with open('last_fetched.txt', 'w') as f:
            f.write(str(timestamp))

    def fetch_files(self):
        self.logger.info("Starting fetch cycle")
        current_time = int(time.time())
        new_files = [f"file_{i}.txt" for i in range(self.last_fetched_file + 1, current_time % 10 + 1)]
        new_files = [f"file_{i}.txt" for i in range(0, 10)]
        if not new_files:
            self.logger.info("No new files to fetch")
            return

        for file in new_files:
            self.logger.info(f"Fetching file: {file}")
            time.sleep(1)  # סימולציית אחזור
            self.rabbitmq.publish(file)

        self._save_last_fetched(current_time)
        self.logger.info("Fetch cycle completed")