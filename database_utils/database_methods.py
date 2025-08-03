import os
import sqlite3
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    """
    A utility class to manage SQLite database operations for storing news articles.

    Attributes:
        DB_DIR (str): Relative directory path where the SQLite database file is located.
        DB_FILE (str): Filename of the SQLite database.
        DB_PATH (str): Full path combining DB_DIR and DB_FILE.
        DEFAULT_TIMESTAMP (str): Default timestamp used when no articles present.
        TABLE_NAME (str): Name of the table to store articles.
    """
     
    def __init__(self, DB_DIR, DB_FILE, DEFAULT_TIMESTAMP = "2025-07-25T00:00:00", TABLE_NAME = "articles"):
        """
        Initialize Database instance and ensure the articles table exists.

        Args:
            DB_DIR (str): Directory path for database file.
            DB_FILE (str): SQLite database filename.
            DEFAULT_TIMESTAMP (str): Starting timestamp in case of full load (Default = "2025-07-25T00:00:00")
            TABLE_NAME (str): Target SQLite table name (Default = "articles")

        """

        self.DB_DIR = DB_DIR
        self.DB_FILE = DB_FILE
        self.DB_PATH = os.path.join(DB_DIR, DB_FILE)
        self.DEFAULT_TIMESTAMP = DEFAULT_TIMESTAMP
        self.TABLE_NAME = TABLE_NAME
        self.create_table_if_not_exist()

    def get_latest_news_time(self):
        """
        Retrieve the latest news published timestamp from the articles table.

        Returns:
            tuple(datetime.datetime, bool): 
                - The latest published timestamp (hour/minute/second zeroed).
                - Boolean indicating if incremental ingestion is possible 
                  (True if any record exists, False if no data present).

        """
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

    def upsert_articles(self, extracted_articles):
        """
        Insert or update articles in the database using upsert logic based on Article_id.

        Args:
            extracted_articles (iterable of dict): List or iterable of article data dictionaries.
                Each dictionary must include:
                  'Article_id', 'News_link', 'News_title', 'Author_name',
                  'News_published_time', and 'Source_name'.

        Logs error if the operation fails.
        """
        try:
            with sqlite3.connect(self.DB_PATH) as conn:
                cursor = conn.cursor()
                for article in extracted_articles:
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
        """
        Query and print the top N latest articles ordered by publish time descending.

        Args:
            n (int, optional): Number of latest articles to fetch. Defaults to 5.

        Logs error if the query fails.
        """
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

    def create_table_if_not_exist(self):
        """
        Create the  table if it does not already exist.

        Ensures the database directory exists before table creation.

        Logs error on failure.
        """
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
        """
        Drop the articles table if it exists.

        Useful to reset or clear the database schema.

        Logs error on failure.
        """
        try:
            with sqlite3.connect(self.DB_PATH) as conn:
                conn.execute(f"DROP TABLE IF EXISTS {self.TABLE_NAME};")
                conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error dropping table {self.TABLE_NAME}: {e}")