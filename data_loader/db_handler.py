# data_loader/db_handler.py
import sqlite3
from common.logger import setup_logger

class DBHandler:
    def __init__(self, db_path):
        self.logger = setup_logger('DBHandler', 'data_loader.log')
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self._initialize_db()

    def _initialize_db(self):
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS files (name TEXT PRIMARY KEY, status TEXT)''')
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS reference (name TEXT PRIMARY KEY)''')
        self.cursor.execute("INSERT OR IGNORE INTO reference (name) VALUES ('file_3.txt')")
        self.conn.commit()
        self.logger.info("Database initialized")

    def load_file(self, file_name):
        try:
            self.cursor.execute("INSERT OR IGNORE INTO files (name, status) VALUES (?, ?)", (file_name, "loaded"))
            self.conn.commit()
            self.logger.info(f"Loaded {file_name} into database")
        except sqlite3.Error as e:
            self.logger.error(f"Failed to load {file_name}: {e}")
            raise

    def check_match(self, file_name):
        self.cursor.execute("SELECT name FROM reference WHERE name = ?", (file_name,))
        match = self.cursor.fetchone()
        return bool(match)

    def close(self):
        self.conn.close()
        self.logger.info("Database connection closed")