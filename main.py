import logging
import json
import schedule
import time
from news_collector import NewsCollector
from content_rewriter import ContentRewriter
from affiliate_linker import AffiliateLinker
from publisher import WordPressPublisher
from social_promoter import SocialPromoter
from optimizer_brain import OptimizerBrain

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("affiliate_agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MainAgent")

def job():
    logger.info("--- DÉMARRAGE DU CYCLE DE L'AGENT ---")
    
    with open('config.json', 'r') as f:
        config = json.load(f)

    # Créer des instances de chaque module
    collector = NewsCollector(config)
    rewriter = ContentRewriter(config)
    linker = AffiliateLinker(config)
    publisher = WordPressPublisher(config)
    promoter = SocialPromoter(config)

    # Exécuter les tâches dans l'ordre
    collector.run()
    rewriter.process_pending_articles()
    linker.process_articles()
    publisher.process_articles()
    promoter.process_promotion()
    
    logger.info("--- FIN DU CYCLE DE L'AGENT ---")

def weekly_optimization():
    logger.info("--- DÉMARRAGE DE L'OPTIMISATION HEBDOMADAIRE ---")
    optimizer = OptimizerBrain()
    optimizer.adjust_strategy()
    logger.info("--- FIN DE L'OPTIMISATION HEBDOMADAIRE ---")

if __name__ == "__main__":
    # Planifier le job principal
    schedule.every(2).hours.do(job)
    # Planifier l'optimisation
    schedule.every().sunday.at("23:00").do(weekly_optimization)

    logger.info("Agent démarré. En attente des tâches planifiées.")
    # Exécuter une fois au démarrage pour tester
    job() 

    while True:
        schedule.run_pending()
        time.sleep(60)
      
