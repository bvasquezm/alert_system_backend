"""
CLI runner to execute the scraper synchronously and send Teams report.
Avoids serverless background/thread issues by running directly in Actions or locally.
"""
import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
import certifi
from pymongo import MongoClient

# Local imports from backend
from src.orchestrator import ScraperOrchestrator
from src.services import teams_service

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("runner")


def get_mongo_client(mongo_uri: str) -> MongoClient:
    """Create a Mongo client with TLS + certifi CA."""
    kwargs = {
        'serverSelectionTimeoutMS': int(os.getenv('MONGO_SERVER_SELECTION_TIMEOUT_MS', '30000')),
        'connectTimeoutMS': int(os.getenv('MONGO_CONNECT_TIMEOUT_MS', '20000')),
        'socketTimeoutMS': int(os.getenv('MONGO_SOCKET_TIMEOUT_MS', '20000')),
        'tls': True,
        'tlsCAFile': certifi.where(),
    }
    return MongoClient(mongo_uri, **kwargs)


def main() -> int:
    mongo_uri = os.getenv('MONGO_URI')
    webhook_url = os.getenv('TEAMS_WEBHOOK_URL')
    config_path = os.getenv('SCRAPER_CONFIG_PATH', 'config_components.json')

    if not mongo_uri:
        logger.error("MONGO_URI env var is required")
        return 2

    try:
        client = get_mongo_client(mongo_uri)
        db = client['scraper_alerts']
        results_collection = db['results']
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        return 3

    logger.info("Starting ScraperOrchestrator run...")
    try:
        orchestrator = ScraperOrchestrator(
            config_path=config_path,
            headless=True,
        )
        report = orchestrator.run()
        report['saved_at'] = datetime.now()
        results_collection.insert_one(report)
        logger.info("Report saved to MongoDB 'results' collection")
    except Exception as e:
        logger.error(f"Scraper execution failed: {e}")
        return 4

    # Teams sending is handled via the API in the workflow to avoid duplication here.
    if webhook_url:
        logger.info("TEAMS_WEBHOOK_URL is set; skipping CLI send.")
    else:
        logger.info("No TEAMS_WEBHOOK_URL set; API step will handle Teams report if configured there.")

    return 0


if __name__ == '__main__':
    sys.exit(main())
