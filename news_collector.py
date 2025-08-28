import os
import logging
import requests
import sqlite3
import random
import time
from bs4 import BeautifulSoup
from datetime import datetime
from pyairtable import Table
from urllib.parse import quote

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("affiliate_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("NewsCollector")

class NewsCollector:
    def __init__(self, config):
        self.config = config
        self.airtable_key = os.getenv("AIRTABLE_API_KEY")
        self.airtable_base_id = os.getenv("AIRTABLE_BASE_ID")
        self.airtable_table_name = "Collected_News"
        self.db_conn = self._init_local_cache()
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15"
        ]

    def _init_local_cache(self):
        conn = sqlite3.connect('news_cache.db')
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS articles_cache (url TEXT PRIMARY KEY, title TEXT, date_collected TEXT)')
        conn.commit()
        return conn

    def _is_in_cache(self, url):
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT url FROM articles_cache WHERE url = ?", (url,))
        return cursor.fetchone() is not None

    def _add_to_cache(self, article):
        cursor = self.db_conn.cursor()
        try:
            cursor.execute("INSERT INTO articles_cache (url, title, date_collected) VALUES (?, ?, ?)",
                           (article["url"], article["title"], datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            self.db_conn.commit()
        except sqlite3.IntegrityError:
            logger.warning(f"URL déjà en cache: {article['url']}")

    def scrape_google_news(self, topic, max_results=5):
        encoded_topic = quote(topic)
        url = f"https://news.google.com/search?q={encoded_topic}&hl=fr&gl=FR"
        headers = {"User-Agent": random.choice(self.user_agents)}
        collected_news = []
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            articles = soup.find_all('div', {'class': 'NiLAwe'})
            for article in articles[:max_results]:
                title_element = article.find('h3', {'class': 'ipQwMb'})
                link_element = article.find('a')
                if not title_element or not link_element or not link_element.has_attr('href'):
                    continue
                
                title = title_element.text.strip()
                article_url = link_element['href']
                
                if article_url.startswith('./'):
                    article_url = f"https://news.google.com{article_url[1:]}"
                
                if self._is_in_cache(article_url):
                    continue
                
                snippet_element = article.find('span', {'class': 'xBbh9'})
                content_snippet = snippet_element.text.strip() if snippet_element else ""

                news_item = {
                    "title": title,
                    "url": article_url,
                    "topic": topic,
                    "status": "collected",
                    "content_raw": content_snippet
                }
                self._add_to_cache(news_item)
                collected_news.append(news_item)
            return collected_news
        except Exception as e:
            logger.error(f"Erreur scraping pour {topic}: {e}")
            return []

    def store_in_airtable(self, news_items):
        if not news_items:
            return
        table = Table(self.airtable_key, self.airtable_base_id, self.airtable_table_name)
        for item in news_items:
            try:
                if not table.first(formula=f"{{url}} = '{item['url']}'"):
                    table.create(item)
                    logger.info(f"Article enregistré dans Airtable: {item['title']}")
            except Exception as e:
                logger.error(f"Erreur Airtable pour {item['title']}: {e}")

    def run(self):
        topics = self.config.get("content", {}).get("topics", ["technologie", "finance"])
        for topic in topics:
            news = self.scrape_google_news(topic)
            self.store_in_airtable(news)
            time.sleep(random.uniform(5, 10))
        self.db_conn.close()
  
