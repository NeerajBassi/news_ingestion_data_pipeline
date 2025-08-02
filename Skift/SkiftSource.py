import requests
from bs4 import BeautifulSoup
from datetime import datetime
from requests.exceptions import RequestException
import logging
import time
import random
from helper_utils.helpers import generate_article_id, drop_timezone, parse_date, datetime_to_iso_with_time

class SkiftScraper:
    def __init__(self, min_delay=1, max_delay=3, max_pages=15, max_retries = 3, backoff_factor= 1.0,timeout = 10 ):
        """
        Initialize the SkiftScraper.
        :param last_ingested_date: datetime.datetime or None, stop scraping older news
        :param min_delay: minimum delay between requests (seconds)
        :param max_delay: maximum delay between requests (seconds)
        :param max_pages: max number of pages to scrape
        """
        self.base_url = "https://skift.com/news/"
        self.headers = {}
        self.source_name = 'Skift'
        self.min_delay = min_delay
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
                print(f"Request failed: {e}. Retrying in {wait:.1f} seconds (Attempt {attempt + 1} of {self.max_retries})")
                time.sleep(wait)
        print(f"Failed to fetch {url} after {self.max_retries} attempts.")
        return None

    def get_page_url(self, page):
        return f"{self.base_url}page/{page}/"

    def extract_articles(self,last_ingested_date):
        page = 1

        while True:
            url = self.get_page_url(page)
            response = self.fetch_url_with_retries(url)

            if not response:
                print(f"Stopping scraping due to repeated request failures at page {page}")
                break

            soup = BeautifulSoup(response.text, "html.parser")
            articles = soup.select("article")

            if not articles:
                print(f"No articles found on page {page}, stopping.")
                break

            stop_paging = False

            for article in articles:
                link_tag = article.select_one("h3.c-tease__title a")
                if not link_tag:
                    print("Article missing title link, skipping.")
                    continue

                news_url = link_tag.get('href')
                if not news_url:
                    print("Article missing href link, skipping.")
                    continue

                article_id = generate_article_id(news_url)
                if article_id in self.seen_article_ids:
                    print(f"Duplicate article {article_id} found, skipping.")
                    continue
                
                headline = link_tag.text.strip() if link_tag else None
                author_tag = article.select_one("div.c-tease__byline a.underline")
                if not author_tag:
                    print(f"Author not available for article id {article_id} and article headline {headline}.")
                author_name = author_tag.text.strip() if author_tag else None

                time_tag = article.select_one("div.c-tease__byline time")            
                news_time = drop_timezone(time_tag.get("datetime")) if time_tag else None
                
                try:
                    news_time = parse_date(news_time)
                except Exception as e:
                    print(f"Error parsing date '{news_time}': {e}")
                    news_time = None
                if news_time :
                    if last_ingested_date and news_time < last_ingested_date:
                        # If last_ingested_date is set and this article is older or equal, stop ingestion
                        stop_paging = True
                        print(f"Encountered article dated {news_time} < last ingested {last_ingested_date}, stopping.")
                        break
                else:
                    # If no date found, you can decide to skip or include
                    print("Article without date found, skipping date check.")
                

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
            print(f"Sleeping for {delay:.1f} seconds before next page request.")
            time.sleep(delay)

        print(f"Total new articles extracted: {len(self.collected_articles)}")
        return self.collected_articles