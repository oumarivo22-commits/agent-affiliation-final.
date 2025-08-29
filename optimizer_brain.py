import logging
import json
import os
from pyairtable import Table
import pandas as pd

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("affiliate_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("OptimizerBrain")

class OptimizerBrain:
    def __init__(self, config_path='config.json'):
        self.config_path = config_path
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        self.airtable_key = os.getenv("AIRTABLE_API_KEY")
        self.airtable_base_id = os.getenv("AIRTABLE_BASE_ID")
        self.airtable_table_name = "Collected_News"

    def fetch_performance_data(self):
        """Récupère les données de performance depuis Airtable."""
        logger.info("Récupération des données de performance depuis Airtable...")
        table = Table(self.airtable_key, self.airtable_base_id, self.airtable_table_name)
        # On ne récupère que les articles publiés pour l'analyse
        records = table.all(formula="status = 'published'")
        
        # Simulation des vues et commissions car non disponibles directement
        # Dans un cas réel, il faudrait une source pour ces données (Google Analytics, API ClickBank)
        data = []
        for record in records:
            fields = record['fields']
            topic = fields.get('topic', 'inconnu')
            # Simuler des données de performance
            views = random.randint(50, 5000)
            commissions = 0
            if "santé" in topic or "finance" in topic:
                commissions = random.uniform(5, 150) * (views / 5000)
            
            data.append({
                "topic": topic,
                "views": views,
                "commissions": commissions,
                "products": fields.get('products_linked', '')
            })
        return pd.DataFrame(data)

    def analyze_performance(self):
        """Analyse les données pour identifier les sujets et produits performants."""
        df = self.fetch_performance_data()
        if df.empty:
            logger.warning("Aucune donnée de performance à analyser.")
            return None, None

        # Analyse par sujet
        topic_performance = df.groupby('topic').agg(
            total_views=('views', 'sum'),
            total_commissions=('commissions', 'sum'),
            article_count=('topic', 'size')
        ).reset_index()
        
        # Calcul d'un score simple
        topic_performance['score'] = (topic_performance['total_views'] * 0.1) + (topic_performance['total_commissions'] * 2)
        top_topics = topic_performance.sort_values(by='score', ascending=False)
        
        logger.info("Performance par sujet :\n" + top_topics.to_string())
        
        # Analyse par produit (simplifiée)
        # Dans un cas réel, il faudrait des tracking IDs précis
        
        return top_topics, None # L'analyse de produit est conceptuelle ici

    def adjust_strategy(self):
        """Ajuste le fichier de configuration basé sur l'analyse."""
        top_topics, _ = self.analyze_performance()
        if top_topics is None:
            return

        # Prend les 3 sujets les plus performants
        new_priority_topics = top_topics['topic'].head(3).tolist()
        
        # Récupérer les sujets actuels de la config
        current_topics = self.config.get("content", {}).get("topics", [])
        
        # Créer la nouvelle liste de sujets en mettant les plus performants en premier
        # et en gardant les autres pour la diversité
        final_topics = new_priority_topics + [t for t in current_topics if t not in new_priority_topics]
        
        logger.info(f"Ancienne priorité de sujets : {current_topics}")
        logger.info(f"Nouvelle priorité de sujets : {final_topics}")
        
        # Mettre à jour la configuration en mémoire
        if "content" not in self.config:
            self.config["content"] = {}
        self.config["content"]["topics"] = final_topics
        
        # Sauvegarder le fichier de configuration
        try:
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
            logger.info("Fichier de configuration mis à jour avec succès.")
        except Exception as e:
            logger.error(f"Impossible de sauvegarder le fichier de configuration : {e}")

