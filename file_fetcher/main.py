# file_fetcher/main.py
from fetcher import FileFetcher
from common.config import FETCHER_QUEUE
import time
import sys,os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
print("sys.path:", sys.path)  # הדפסה זמנית לבדיקה


def main():
    fetcher = FileFetcher(queue_name=FETCHER_QUEUE)
    try:
        while True:
            try:
                fetcher.fetch_files()
                time.sleep(10)
            except Exception as e:
                fetcher.logger.error(f"Error in fetch loop: {e}")
                time.sleep(5)
    except KeyboardInterrupt:
        fetcher.logger.info("Stopped by user")
    finally:
        fetcher.rabbitmq.close()  # סגירה רק בסיום התוכנית

if __name__ == "__main__":
    main()