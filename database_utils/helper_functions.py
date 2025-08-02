def get_latest_news_time():
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.execute("SELECT MAX(News_published_time) FROM articles")
        result = cursor.fetchone()
        if result[0]:
            incremental = True
            latest_timestamp = result[0]
        else:
            incremental = False
            latest_timestamp = '2025-07-25T00:00:00'

        latest_timestamp = datetime.fromisoformat(latest_timestamp).replace(hour=0, minute=0, second=0, microsecond=0)
        return (latest_timestamp, incremental)