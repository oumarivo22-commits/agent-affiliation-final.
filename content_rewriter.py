import os
import time
import sqlite3
import logging
import hashlib
import json
from openai import OpenAI
from pyairtable import Table

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("affiliate_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ContentRewriter")

class ContentRewriter:
    def __init__(self, config):
        self.config = config
        self.airtable_key = os.getenv("AIRTABLE_API_KEY")
        self.airtable_base_id = os.getenv("AIRTABLE_BASE_ID")
        self.airtable_table_name = "Collected_News"
        self.client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=os.getenv("OPENROUTER_API_KEY"),
        )
        self.text_models = self.config.get("models", {}).get("text_generation", [])
        self.db_conn = self._init_rewrite_cache()

    def _init_rewrite_cache(self):
        conn = sqlite3.connect('rewrite_cache.db')
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS rewrite_cache (
            content_hash TEXT PRIMARY KEY,
            rewritten_content TEXT,
            model_used TEXT,
            success INTEGER
        )
        ''')
        conn.commit()
        return conn

    def _get_content_hash(self, content, title, topic):
        return hashlib.md5(f"{content}_{title}_{topic}".encode()).hexdigest()

    def _check_rewrite_cache(self, content, title, topic):
        content_hash = self._get_content_hash(content, title, topic)
        cursor = self.db_conn.cursor()
        cursor.execute("SELECT rewritten_content, model_used FROM rewrite_cache WHERE content_hash = ? AND success = 1", (content_hash,))
        result = cursor.fetchone()
        return result if result else (None, None)

    def _add_to_rewrite_cache(self, original_content, rewritten_content, title, topic, model_used, success=True):
        content_hash = self._get_content_hash(original_content, title, topic)
        cursor = self.db_conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO rewrite_cache (content_hash, rewritten_content, model_used, success) VALUES (?, ?, ?, ?)",
                       (content_hash, rewritten_content, model_used, 1 if success else 0))
        self.db_conn.commit()

    def rewrite_content_with_fallback(self, original_content, title, topic):
        cached_content, cached_model = self._check_rewrite_cache(original_content, title, topic)
        if cached_content:
            logger.info(f"Contenu trouvé dans le cache (généré par {cached_model}) pour: {title}")
            return cached_content

        prompt = f"""
        Tu es un expert en rédaction SEO. Réécris complètement cet article 
        sur le sujet {topic} en français. Rends-le unique, informatif et optimisé pour le SEO.
        
        Titre original: {title}
        Contenu original à réécrire: {original_content}
        
        Instructions clés :
        - Style : Professionnel mais engageant.
        - Structure : Introduction, plusieurs sous-titres (H2), et une conclusion.
        - Ne te contente pas de paraphraser, apporte une nouvelle perspective.
        """

        for model in self.text_models:
            try:
                logger.info(f"Tentative de réécriture avec le modèle : {model}")
                completion = self.client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "Tu es un expert en rédaction SEO."},
                        {"role": "user", "content": prompt},
                    ]
                )
                rewritten_content = completion.choices[0].message.content
                if rewritten_content and len(rewritten_content) > 200:
                    logger.info(f"Réécriture réussie avec {model}.")
                    self._add_to_rewrite_cache(original_content, rewritten_content, title, topic, model, success=True)
                    return rewritten_content
                else:
                    logger.warning(f"Le modèle {model} a retourné un contenu trop court.")
            except Exception as e:
                logger.warning(f"Échec avec le modèle {model}: {e}. Essai du modèle suivant.")
                continue
        
        logger.error(f"Échec de la réécriture pour '{title}' avec tous les modèles.")
        self._add_to_rewrite_cache(original_content, "", title, topic, "all_failed", success=False)
        return None

    def process_pending_articles(self):
        table = Table(self.airtable_key, self.airtable_base_id, self.airtable_table_name)
        records = table.all(formula="status = 'collected'")
        logger.info(f"{len(records)} articles trouvés à réécrire.")
        for record in records:
            fields = record['fields']
            title = fields.get('title', 'Sans titre')
            logger.info(f"Traitement de l'article : {title}")
            content = self.rewrite_content_with_fallback(fields.get('content_raw', ''), title, fields.get('topic', 'général'))
            if content:
                table.update(record['id'], {"status": "rewritten", "content_rewritten": content})
                logger.info(f"Article '{title}' mis à jour dans Airtable avec le statut 'rewritten'.")
            else:
                table.update(record['id'], {"status": "error_rewrite", "error": "Échec de tous les modèles de réécriture"})
                logger.error(f"Impossible de mettre à jour l'article '{title}' car la réécriture a échoué.")
        self.db_conn.close()
  
