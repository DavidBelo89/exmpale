# file_processor/main.py
from processor import FileProcessor
from common.config import LOADER_QUEUE

def main():
    processor = FileProcessor(queue_name=LOADER_QUEUE)
    try:
        processor.start()
    except KeyboardInterrupt:
        processor.logger.info("Stopped by user")
        processor.rabbitmq.close()

if __name__ == "__main__":
    main()