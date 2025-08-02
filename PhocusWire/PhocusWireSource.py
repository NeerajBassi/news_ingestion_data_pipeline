import requests
from bs4 import BeautifulSoup
from datetime import datetime
from requests.exceptions import RequestException
import logging
import time
import random
from helper_utils.helpers import generate_article_id, drop_timezone, parse_date, datetime_to_iso_with_time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PhocuswireScraper:
    def __init__(self, min_delay=1, max_delay=3, max_pages=15, max_retries = 3, backoff_factor= 1.0,timeout = 10 ):
        """
        Initialize the PhocusewireScraper.
        :param min_delay: minimum delay between requests (seconds)
        :param max_delay: maximum delay between requests (seconds)
        :param max_pages: max number of pages to scrape
        """
        self.base_url = "https://www.phocuswire.com"
        self.headers =  {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                    'AppleWebKit/537.36 (KHTML, like Gecko) '
                                    'Chrome/115.0.0.0 Safari/537.36'
                        }
        self.min_delay = min_delay
        self.source_name = "Phocuswire"
        self.max_delay = max_delay
        self.max_pages = max_pages
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout = timeout
        self.collected_articles = []
        self.seen_article_ids = set()

    def fetch_url_with_retries(self, url):
        for attempt in range(self.max_retries):
            try:
                if self.headers: 
                    response = requests.get(url, timeout=self.timeout, headers=self.headers)
                else:
                    response = requests.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response
            except RequestException as e:
                wait = self.backoff_factor * (2 ** attempt)
                logger.error(f"Request failed: {e}. Retrying in {wait:.1f} seconds (Attempt {attempt + 1} of {self.max_retries})")
                time.sleep(wait)
        logger.error(f"Failed to fetch {url} after {self.max_retries} attempts.")
        return None

    def get_page_url(self, page):
        return f"{self.base_url}/Latest-News?pg={page}"

    def extract_articles(self,last_ingested_date):
        page = 1

        while True:
            url = self.get_page_url(page)
            response = self.fetch_url_with_retries(url)

            if not response:
                logger.info(f"Stopping scraping due to repeated request failures at page {page}")
                break

            soup = BeautifulSoup(response.text, "html.parser")
            articles = soup.select("div.article-list  div.item")

            if not articles:
                logger.info(f"No articles found on page {page}, stopping.")
                break

            stop_paging = False

            for article in articles:
                # Extract news date (inside div.author)
                title_tag = article.select_one("a.title")
                if not title_tag:
                    logger.info("Article missing title link, skipping.")
                    continue

                headline = title_tag.get_text(strip=True) if title_tag else None

                if not title_tag['href']:
                    logger.info("Article missing href link, skipping.")
                    continue

                news_url = f"{self.base_url}/{title_tag['href']}" if title_tag and 'href' in title_tag.attrs else None

                article_id = generate_article_id(news_url) if news_url else None
                if article_id in self.seen_article_ids:
                    logger.info(f"Duplicate article {article_id} found, skipping.")
                    continue

                author_span = article.select_one("div.author > span.name")
                if not author_span:
                    logger.info(f"Author not available for article id {article_id} and article headline {headline}.")
                author_name = author_span.get_text(strip=True).replace("By ", "") if author_span else None
                
                # Extract news time from div.author text after the pipe symbol
                author_div = article.select_one("div.author")
                news_time = None
                if author_div:
                    # The text looks like 'By Abby Crotty | July 28, 2025'
                    # We can split by '|' and strip whitespace to get news time
                    parts = author_div.text.split('|')
                    if len(parts) == 2:
                        news_time = parts[1].strip()
                        
                    try:
                        news_time = parse_date(news_time, self.source_name)
                    except Exception as e:
                        logger.error(f"Error parsing date '{news_time}': {e}")
                        news_time = None
                if news_time :
                    if last_ingested_date and news_time < last_ingested_date:
                        # If last_ingested_date is set and this article is older or equal, stop ingestion
                        stop_paging = True
                        logger.info(f"Encountered article dated {news_time} < last ingested {last_ingested_date}, stopping.")
                        break
                else:
                    # If no date found, you can decide to skip or include
                    logger.info("Article without date found, skipping date check.")

                
                article_data = {
                    "Article_id": article_id,
                    "News_title": headline,
                    "News_link": news_url,
                    "Author_name": author_name,
                    "News_published_time": datetime_to_iso_with_time(news_time),
                    "Source_name": self.source_name
                }

                self.collected_articles.append(article_data)
                self.seen_article_ids.add(article_id)

            if stop_paging:
                break

            page += 1
            delay = random.uniform(self.min_delay, self.max_delay)
            logger.info(f"Sleeping for {delay:.1f} seconds before next page request.")
            time.sleep(delay)

        logger.info(f"Total new articles extracted: {len(self.collected_articles)}")
        return self.collected_articles