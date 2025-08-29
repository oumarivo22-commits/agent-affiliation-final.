import os
import logging
import requests
import sqlite3
import random
import time
import json
import base64
from openai import OpenAI
from pyairtable import Table
import markdown
from bs4 import BeautifulSoup

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("affiliate_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("Publisher")

class WordPressPublisher:
    def __init__(self, config):
        self.config = config
        self.airtable_key = os.getenv("AIRTABLE_API_KEY")
        self.airtable_base_id = os.getenv("AIRTABLE_BASE_ID")
        self.airtable_table_name = "Collected_News"
        self.wp_url = os.getenv("WORDPRESS_URL")
        self.wp_user = os.getenv("WORDPRESS_USER")
        self.wp_app_password = os.getenv("WORDPRESS_APP_PASSWORD")
        self.image_models = self.config.get("models", {}).get("image_generation", [])
        self.client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=os.getenv("OPENROUTER_API_KEY"))
        self.db_conn = self._init_publication_cache()

    def _init_publication_cache(self):
        conn = sqlite3.connect('publication_cache.db')
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS published_articles (airtable_id TEXT PRIMARY KEY, wp_post_id INTEGER, permalink TEXT)')
        conn.commit()
        return conn

    def _check_publication_cache(self, airtable_id):
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT wp_post_id FROM published_articles WHERE airtable_id = ?", (airtable_id,))
        return cursor.fetchone()

    def _add_to_publication_cache(self, airtable_id, wp_post_id, permalink):
        cursor = self.db_conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO published_articles (airtable_id, wp_post_id, permalink) VALUES (?, ?, ?)", (airtable_id, wp_post_id, permalink))
        self.db_conn.commit()

    def generate_image_with_fallback(self, title, topic):
        prompt = f"Une image professionnelle et pertinente pour un article de blog sur '{topic}' intitulé '{title}'. Style : photographie haute qualité."
        for model in self.image_models:
            try:
                logger.info(f"Génération d'image avec {model}")
                response = self.client.images.generate(model=model, prompt=prompt, n=1, size="1024x1024")
                if response.data and response.data[0].url:
                    image_url = response.data[0].url
                    image_response = requests.get(image_url)
                    if image_response.status_code == 200:
                        return image_response.content, model
            except Exception as e:
                logger.warning(f"Échec de génération d'image avec {model}: {e}")
                continue
        return None, None

    def upload_featured_image(self, image_data, title):
        if not image_data: return None
        auth = base64.b64encode(f"{self.wp_user}:{self.wp_app_password}".encode()).decode()
        headers = {
            'Content-Disposition': f'attachment; filename="featured_{int(time.time())}.jpg"',
            'Content-Type': 'image/jpeg',
            'Authorization': f'Basic {auth}'
        }
        url = f"{self.wp_url}/wp-json/wp/v2/media"
        try:
            response = requests.post(url, headers=headers, data=image_data, timeout=60)
            response.raise_for_status()
            return response.json()['id']
        except Exception as e:
            logger.error(f"Erreur upload image: {e}")
            return None

    def markdown_to_html(self, markdown_content):
        html = markdown.markdown(markdown_content)
        soup = BeautifulSoup(html, 'html.parser')
        for a in soup.find_all('a'):
            if 'hop.clickbank.net' in a.get('href', ''):
                a['target'] = '_blank'
                a['rel'] = 'noopener noreferrer sponsored'
        return str(soup)

    def create_post(self, title, content, featured_image_id=None):
        auth = base64.b64encode(f"{self.wp_user}:{self.wp_app_password}".encode()).decode()
        headers = {'Authorization': f'Basic {auth}', 'Content-Type': 'application/json'}
        post_data = {'title': title, 'content': content, 'status': 'publish'}
        if featured_image_id:
            post_data['featured_media'] = featured_image_id
        
        url = f"{self.wp_url}/wp-json/wp/v2/posts"
        try:
            response = requests.post(url, headers=headers, json=post_data, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Erreur création post: {e}")
            return None

    def process_articles(self):
        table = Table(self.airtable_key, self.airtable_base_id, self.airtable_table_name)
        records = table.all(formula="status = 'monetized'")
        logger.info(f"{len(records)} articles trouvés à publier.")
        for record in records:
            if self._check_publication_cache(record['id']):
                logger.info(f"Article {record['id']} déjà publié.")
                continue

            fields = record['fields']
            title = fields.get('title', 'Sans titre')
            logger.info(f"Publication de l'article : {title}")

            image_data, model_used = self.generate_image_with_fallback(title, fields.get('topic', ''))
            image_id = self.upload_featured_image(image_data, title)
            
            html_content = self.markdown_to_html(fields.get('content_monetized', ''))
            post_response = self.create_post(title, html_content, image_id)

            if post_response:
                permalink = post_response.get('link')
                self._add_to_publication_cache(record['id'], post_response['id'], permalink)
                table.update(record['id'], {"status": "published", "wp_permalink": permalink})
                logger.info(f"Article '{title}' publié avec succès sur {permalink}")
        self.db_conn.close()
      
