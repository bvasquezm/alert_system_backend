"""
CLI runner to execute the scraper synchronously and send Teams report.
Avoids serverless background/thread issues by running directly in Actions or locally.
"""
import os
import sys
import logging
import time
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


def _get_int_env(var_name: str, default: int) -> int:
    value = os.getenv(var_name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning(f"Invalid value for {var_name}='{value}', using default {default}")
        return default


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
    max_workers = _get_int_env('SCRAPER_MAX_WORKERS', 3)
    max_retries = _get_int_env('SCRAPER_MAX_RETRIES', 2)
    retry_backoff_seconds = _get_int_env('SCRAPER_RETRY_BACKOFF_SECONDS', 10)
    playwright_timeout_ms = _get_int_env(
        'PLAYWRIGHT_TIMEOUT_MS',
        _get_int_env('SCRAPER_PLAYWRIGHT_TIMEOUT_MS', 45000)
    )

    os.environ['PLAYWRIGHT_TIMEOUT_MS'] = str(playwright_timeout_ms)
    logger.info(
        f"Runner config -> max_workers={max_workers}, retries={max_retries}, "
        f"backoff={retry_backoff_seconds}s, playwright_timeout_ms={playwright_timeout_ms}"
    )

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
    report = None
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Scraper attempt {attempt}/{max_retries}")
            orchestrator = ScraperOrchestrator(
                config_path=config_path,
                headless=True,
                max_workers=max_workers
            )
            report = orchestrator.run()
            report['saved_at'] = datetime.now()
            results_collection.insert_one(report)
            logger.info("Report saved to MongoDB 'results' collection")

            failed = report.get('failed', 0)
            total = report.get('total_countries', 0)
            logger.info(
                f"Run completed: successful={report.get('successful', 0)}/{total}, "
                f"failed={failed}, total_alerts={report.get('total_alerts', 0)}"
            )
            break
        except Exception as e:
            last_error = e
            logger.error(f"Scraper execution attempt {attempt} failed: {e}")

            if attempt < max_retries:
                wait_seconds = retry_backoff_seconds * attempt
                logger.info(f"Retrying in {wait_seconds} seconds...")
                time.sleep(wait_seconds)

    if report is None:
        logger.error(f"Scraper execution failed after {max_retries} attempts: {last_error}")
        return 4

    # Teams sending is handled via the API in the workflow to avoid duplication here.
    if webhook_url:
        logger.info("TEAMS_WEBHOOK_URL is set; skipping CLI send.")
    else:
        logger.info("No TEAMS_WEBHOOK_URL set; API step will handle Teams report if configured there.")

    return 0


if __name__ == '__main__':
    sys.exit(main())
