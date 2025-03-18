# data_loader/main.py
from loader import DataLoader
from common.config import FETCHER_QUEUE, LOADER_QUEUE, DB_PATH

def main():
    loader = DataLoader(input_queue=FETCHER_QUEUE, output_queue=LOADER_QUEUE, db_path=DB_PATH)
    try:
        loader.start()
    except KeyboardInterrupt:
        loader.logger.info("Stopped by user")
        loader.input_rabbitmq.close()
        loader.output_rabbitmq.close()
        loader.db.close()

if __name__ == "__main__":
    main()