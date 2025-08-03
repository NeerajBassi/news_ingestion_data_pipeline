# News Articles Ingestion Project

## Overview

This project implements a robust web scraping and ingestion pipeline to collect news articles from multiple sources (e.g., Skift, PhocusWire), process them, and store them in a local SQLite database and displays the top 5 (configurable) latest articles sorted by news article publication time.

Main pipeline execution code is in `ingestion.ipynb`

## 1. Ingestion

The ingestion process involves web scraping paginated news article pages from multiple sources. Each source has its own scraper class implementing:

- URL construction with pagination
- HTTP requests with retries and randomized delays for preventing rate limits.
- Headers defined for requests.
- Parsing HTML content with BeautifulSoup to extract article elements.
- Extraction of article metadata including:
  - News URL
  - Article ID (generated from URL)
  - Headline (news title)
  - News link (URL)
  - Author name
  - Publish timestamp (processed and standardized)
  - Source name

After extraction, articles are filtered to avoid duplicates and then inserted/upserted into the SQLite database.

## 2. Schema of Target Table (`articles`)

The target SQLite table `articles` has the following schema:

| Column Name          | Data Type | Description                        |
|----------------------|-----------|----------------------------------|
| `Article_id`         | TEXT      | Primary key, unique article ID   |
| `News_link`          | TEXT      | URL link to the news article     |
| `News_title`         | TEXT      | Headline/title of the article    |
| `Author_name`        | TEXT      | Name of the author (if available)|
| `News_published_time`| TEXT      | Publish timestamp (ISO8601 string)|
| `Source_name`        | TEXT      | Source site of the article (Skift, Phocuswire) |      |
| `Processed_at`       | TEXT      | Timestamp when record was processed|

**Constraints and Notes:**

- `Article_id` is the PRIMARY KEY to avoid duplicate entries.
- Upsert logic updates existing records when an article with the same `Article_id` already exists.
- `Processed_at` tracks when the record was last inserted or updated.

## 3. Incremental Strategy

To efficiently ingest only new or updated articles and reduce redundant workload, the pipeline uses an incremental ingestion strategy:

- The ingestion checks the latest published news timestamp from the target database table (`articles`) called `latest_date`
  - If target database table is empty, full load is intitated with following values.
    -   `latest_timestamp` : `2025-07-25T00:00:00`
    -   `is_incremental` : False
  - If target database table contains data, incremental load is initiated with following values.
    -    `latest_timestamp` : `MAX(News_published_time)` (time part is set zero)
    -   `is_incremental` : True
- Time part is set zero from `latest_timestamp` and only date part is used because articles in **PhocusWire** website are published with date of publication.
- While scraping, articles with published timestamps older to the `latest_timestamp` are ignored.
- Pagination stops once an article older than `latest_timestamp` is encountered.
- On each run, duplicate articles are ignored on the basis of `article_id` (hexadecimal string value created from URL).
- Finally, articles are upserted in the target table with new `article_id` inserted and existing  `article_id` being updated and corresponding `Processed_at`.
- This strategy will update the `Processed_at` of articles for the `latest_date` even they exist already  and no change preventing data loss for the **PhocusWire** articles .


## 4. Robustness & Reliability Steps

The pipeline incorporates several features to ensure robustness:

- **Retry Logic with Backoff:** HTTP requests are retried up to 3 times with exponential backoff on failures.
- **Randomized Request Delays:** Delays (`min_delay` and `max_delay`) between requests reduce risk of server blocks.
- **Duplicate Detection:** Articles are deduplicated using `Article_id` sets during scraping and via the primary key in DB with upsert.
- **Error Handling:** Database operations are wrapped with try-except blocks and errors logged cleanly.
- **Timezone Handling and Date Parsing:** Datetimes are parsed and standardized to comparable formats even if source formats vary.
- **Logging:** Informative logs with timestamps and levels are used for debugging and monitoring.

## 5. Output Format

- The final ingested data is stored in a local **SQLite** database file located at a configurable path.
- Extracted article timestamps are stored in ISO8601 string format (`YYYY-MM-DDTHH:MM:SS`) in the DB.
- Output is given by `query_topn_articles(n = 5)` method with default n = 5.
- Output rows are displayed in format
  
   `(Article_id, News_link, News_title, Author_name, News_published_time, Source_name, Processed_at)`

