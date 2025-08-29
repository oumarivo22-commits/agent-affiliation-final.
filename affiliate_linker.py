import os
import logging
import requests
import sqlite3
import random
import time
import re
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
logger = logging.getLogger("AffiliateLinker")

class AffiliateLinker:
    def __init__(self, config):
        self.config = config
        self.airtable_key = os.getenv("AIRTABLE_API_KEY")
        self.airtable_base_id = os.getenv("AIRTABLE_BASE_ID")
        self.airtable_table_name = "Collected_News"
        self.clickbank_account = os.getenv("CLICKBANK_ACCOUNT")
        self.db_conn = self._init_product_cache()
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Safari/605.1.15"
        ]
        self.categories = {
            "santé": "health-and-fitness", "fitness": "health-and-fitness", "nutrition": "health-and-fitness",
            "finance": "business-and-investing", "investissement": "business-and-investing",
            "technologie": "computers-and-internet", "tech": "computers-and-internet",
            "développement personnel": "self-help"
        }
        self.insertion_templates = [
            "Pour approfondir ce sujet, {product_name} est une excellente ressource. [En savoir plus]",
            "Si vous voulez aller plus loin, je recommande vivement {product_name}. [Découvrir]",
            "Un outil qui pourrait vous aider est {product_name}. [Voir les détails]"
        ]

    def _init_product_cache(self):
        conn = sqlite3.connect('affiliate_products.db')
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS clickbank_products (
            product_id TEXT PRIMARY KEY, name TEXT, description TEXT, category TEXT,
            gravity REAL, hoplink TEXT, keywords TEXT, date_scraped TEXT
        )
        ''')
        conn.commit()
        return conn

    def _get_cached_products(self, category):
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT name, hoplink, keywords FROM clickbank_products WHERE category = ? ORDER BY gravity DESC LIMIT 10", (category,))
        products = [{"name": row[0], "hoplink": row[1], "keywords": row[2].split(',')} for row in cursor.fetchall()]
        return products

    def scrape_clickbank_marketplace(self, category):
        # NOTE: Le scraping direct de ClickBank est difficile et peu fiable.
        # Cette fonction est un exemple conceptuel. Une API serait préférable.
        logger.info(f"Recherche de produits pour la catégorie: {category} (simulation)")
        # Simulation de produits pour éviter le blocage par scraping
        simulated_products = [
            {"name": f"Super Produit {category} 1", "hoplink": f"https://hop.clickbank.net/?affiliate={self.clickbank_account}&vendor=produit1", "keywords": [category, "guide", "solution"]},
            {"name": f"Guide Ultime {category}", "hoplink": f"https://hop.clickbank.net/?affiliate={self.clickbank_account}&vendor=produit2", "keywords": [category, "expert", "méthode"]}
        ]
        return simulated_products

    def _extract_keywords(self, text):
        text = re.sub(r'[^\w\s]', '', text.lower())
        words = text.split()
        stopwords = {'le', 'la', 'les', 'un', 'une', 'de', 'et', 'en', 'pour', 'que'}
        return list(set([word for word in words if len(word) > 3 and word not in stopwords]))

    def find_relevant_products(self, content, title, topic):
        category_key = topic.lower()
        cb_category = self.categories.get(category_key, "self-help")
        products = self.scrape_clickbank_marketplace(cb_category)
        if not products: return []

        content_keywords = self._extract_keywords(content + " " + title)
        scored_products = []
        for product in products:
            score = len(set(content_keywords) & set(product['keywords']))
            if score > 0:
                scored_products.append((product, score))
        
        scored_products.sort(key=lambda x: x[1], reverse=True)
        return [p[0] for p in scored_products[:2]]

    def insert_affiliate_links(self, content, products, article_id):
        if not products: return content
        paragraphs = content.split('\n\n')
        
        for i, product in enumerate(products):
            insert_index = len(paragraphs) // 2 + i * (len(paragraphs) // 4)
            if insert_index < len(paragraphs):
                template = random.choice(self.insertion_templates)
                insertion_text = template.format(product_name=product["name"])
                hoplink = product["hoplink"].replace("{article_id}", str(article_id))
                link_text_match = re.search(r'\[(.*?)\]', insertion_text)
                if link_text_match:
                    link_text = link_text_match.group(1)
                    final_text = re.sub(r'\[(.*?)\]', f'[{link_text}]({hoplink})', insertion_text)
                    paragraphs.insert(insert_index, final_text)
        
        return "\n\n".join(paragraphs)

    def process_articles(self):
        table = Table(self.airtable_key, self.airtable_base_id, self.airtable_table_name)
        records = table.all(formula="status = 'rewritten'")
        logger.info(f"{len(records)} articles trouvés à monétiser.")
        for record in records:
            fields = record['fields']
            title = fields.get('title', 'Sans titre')
            logger.info(f"Monétisation de l'article : {title}")
            
            relevant_products = self.find_relevant_products(fields.get('content_rewritten', ''), title, fields.get('topic', 'général'))
            monetized_content = self.insert_affiliate_links(fields.get('content_rewritten', ''), relevant_products, record['id'])
            
            product_names = [p["name"] for p in relevant_products]
            update_data = {
                "status": "monetized",
                "content_monetized": monetized_content,
                "products_linked": ", ".join(product_names) if product_names else "Aucun"
            }
            table.update(record['id'], update_data)
            logger.info(f"Article '{title}' monétisé avec {len(relevant_products)} lien(s).")
        self.db_conn.close()
      
