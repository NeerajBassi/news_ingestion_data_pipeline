import os
import sqlite3
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, DB_DIR, DB_FILE):
        self.DB_DIR = DB_DIR
        self.DB_FILE = DB_FILE
        self.DB_PATH = os.path.join(DB_DIR, DB_FILE)
        self.DEFAULT_TIMESTAMP = "2025-07-25T00:00:00"
        self.TABLE_NAME = "articles"

    def get_latest_news_time(self):
        try:
            with sqlite3.connect(self.DB_PATH) as conn:
                cursor = conn.execute(f"SELECT MAX(News_published_time) FROM {self.TABLE_NAME}")
                result = cursor.fetchone()
            if result[0]:
                incremental = True
                latest_timestamp = result[0]
            else:
                incremental = False
                latest_timestamp = self.DEFAULT_TIMESTAMP

            latest_timestamp = datetime.fromisoformat(latest_timestamp).replace(hour=0, minute=0, second=0, microsecond=0)
            return (latest_timestamp, incremental)

        except sqlite3.Error as e:
            logger.error(f"Error fetching latest news time: {e}")
            # Depending on your use case, decide what to return on error:
            return (datetime.fromisoformat(self.DEFAULT_TIMESTAMP), False)

    def upsert_articles(self, filtered_articles):
        try:
            with sqlite3.connect(self.DB_PATH) as conn:
                cursor = conn.cursor()
                for article in filtered_articles:
                    sql = f"""
                    INSERT INTO {self.TABLE_NAME} (Article_id,  News_link, News_title, Author_name, News_published_time, Source_name, Processed_at)
                    VALUES (?, ?,  ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(Article_id) DO UPDATE SET
                        News_link = excluded.News_link,
                        News_title = excluded.News_title,
                        Author_name = excluded.Author_name,
                        News_published_time = excluded.News_published_time,
                        Source_name = excluded.Source_name,
                        Processed_at = CURRENT_TIMESTAMP
                    """
                    params = (
                        article.get('Article_id'),
                        article.get('News_link'),
                        article.get('News_title'),
                        article.get('Author_name'),
                        article.get('News_published_time'),
                        article.get('Source_name')
                    )
                    cursor.execute(sql, params)
                conn.commit()

        except sqlite3.Error as e:
            logger.error(f"Error upserting articles: {e}")

    def query_topn_articles(self, n=5):
        try:
            with sqlite3.connect(self.DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute(f"""
                    SELECT Article_id, News_link, News_title, Author_name, News_published_time, Source_name, Processed_at 
                    FROM {self.TABLE_NAME}
                    ORDER BY News_published_time DESC LIMIT ?
                """, (n,))
                rows = cursor.fetchall()
                for row in rows:
                    print(row)
        except sqlite3.Error as e:
            logger.error(f"Error querying top {n} articles: {e}")

    def create_table(self):
        try:
            os.makedirs(self.DB_DIR, exist_ok=True)
            with sqlite3.connect(self.DB_PATH) as conn:
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
                        Article_id TEXT PRIMARY KEY,
                        News_link TEXT,
                        News_title TEXT,
                        Author_name TEXT,
                        News_published_time TEXT,
                        Source_name TEXT,
                        Processed_at TEXT
                    )
                """)
        except Exception as e:
            logger.error(f"Error creating table {self.TABLE_NAME}: {e}")

    def drop_table(self):
        try:
            with sqlite3.connect(self.DB_PATH) as conn:
                conn.execute(f"DROP TABLE IF EXISTS {self.TABLE_NAME};")
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error dropping table {self.TABLE_NAME}: {e}")

    def truncate_table_and_vacuum(self):
        try:
            with sqlite3.connect(self.DB_PATH) as conn:
                conn.execute(f"DELETE FROM {self.TABLE_NAME};")
                conn.commit()
                conn.execute("VACUUM;")
        except sqlite3.Error as e:
            logger.error(f"Error truncating table {self.TABLE_NAME} and vacuuming: {e}")
