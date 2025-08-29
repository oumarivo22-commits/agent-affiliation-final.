import os
import logging
import time
import random
from openai import OpenAI
from pyairtable import Table
import tweepy

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("affiliate_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("SocialPromoter")

class SocialPromoter:
    def __init__(self, config):
        self.config = config
        self.airtable_key = os.getenv("AIRTABLE_API_KEY")
        self.airtable_base_id = os.getenv("AIRTABLE_BASE_ID")
        self.airtable_table_name = "Collected_News"
        
        # Initialisation du client Twitter/X
        try:
            self.twitter_client = tweepy.Client(
                bearer_token=os.getenv("TWITTER_BEARER_TOKEN"),
                consumer_key=os.getenv("TWITTER_API_KEY"),
                consumer_secret=os.getenv("TWITTER_API_SECRET"),
                access_token=os.getenv("TWITTER_ACCESS_TOKEN"),
                access_token_secret=os.getenv("TWITTER_ACCESS_SECRET")
            )
            # Test de l'authentification
            me = self.twitter_client.get_me()
            logger.info(f"Connecté à Twitter en tant que {me.data.username}")
        except Exception as e:
            logger.error(f"Impossible d'initialiser le client Twitter: {e}")
            self.twitter_client = None

        self.openrouter_client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=os.getenv("OPENROUTER_API_KEY"))
        self.text_models = self.config.get("models", {}).get("text_generation", [])

    def get_articles_to_promote(self):
        table = Table(self.airtable_key, self.airtable_base_id, self.airtable_table_name)
        # Cherche les articles publiés qui n'ont pas encore de statut de promotion
        return table.all(formula="AND(status = 'published', promotion_status IS NULL())")

    def generate_tweet_content(self, title, topic):
        prompt = f"Crée un tweet court et percutant (max 280 caractères) pour un article de blog. Inclus 2-3 hashtags pertinents.\nTitre: \"{title}\"\nSujet: {topic}"
        for model in self.text_models:
            try:
                logger.info(f"Génération de tweet avec le modèle : {model}")
                completion = self.openrouter_client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=100,
                    temperature=0.7
                )
                return completion.choices[0].message.content.strip('"')
            except Exception as e:
                logger.warning(f"Échec de la génération de tweet avec {model}: {e}")
                continue
        logger.error("Tous les modèles de génération de tweet ont échoué.")
        return None

    def post_tweet(self, tweet_text, article_url):
        if not self.twitter_client:
            logger.error("Client Twitter non disponible. Publication annulée.")
            return False
            
        full_tweet = f"{tweet_text}\n\nLisez l'article complet ici : {article_url}"
        try:
            response = self.twitter_client.create_tweet(text=full_tweet)
            logger.info(f"Tweet publié avec succès ! ID: {response.data['id']}")
            return True
        except Exception as e:
            logger.error(f"Échec de la publication du tweet : {e}")
            return False

    def process_promotion(self):
        articles = self.get_articles_to_promote()
        logger.info(f"{len(articles)} articles à promouvoir trouvés.")
        
        if not self.twitter_client:
            logger.error("Promotion impossible car le client Twitter n'est pas configuré.")
            return

        for article in articles:
            fields = article['fields']
            title = fields.get('title')
            permalink = fields.get('wp_permalink')

            if not permalink:
                logger.warning(f"Pas de permalien pour l'article '{title}', promotion ignorée.")
                continue

            logger.info(f"Promotion de l'article : {title}")
            tweet_content = self.generate_tweet_content(title, fields.get('topic'))
            
            if tweet_content:
                if self.post_tweet(tweet_content, permalink):
                    # Mettre à jour Airtable pour marquer comme promu
                    table = Table(self.airtable_key, self.airtable_base_id, self.airtable_table_name)
                    table.update(article['id'], {"promotion_status": "done"})
                    # Pause aléatoire pour un comportement plus humain
                    sleep_duration = random.randint(900, 3600) # Pause entre 15 et 60 minutes
                    logger.info(f"Pause de {sleep_duration // 60} minutes avant le prochain tweet.")
                    time.sleep(sleep_duration)
            else:
                logger.error(f"Impossible de générer un tweet pour l'article '{title}'.")
          
