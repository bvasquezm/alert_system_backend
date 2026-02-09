"""
Servicio para gestionar el scraping
"""
from datetime import datetime
import logging
from typing import Dict

logger = logging.getLogger(__name__)


def run_scraper(orchestrator_class, config_path: str, mongo_uri: str, scraper_state: Dict) -> Dict:
    """
    Ejecutar el scraper en background
    
    Args:
        orchestrator_class: Clase ScraperOrchestrator
        config_path: Ruta al archivo de configuración
        mongo_uri: URI de MongoDB
        scraper_state: Diccionario compartido con el estado del scraper
    
    Returns:
        Diccionario con el resultado del scraping
    """
    scraper_state['is_running'] = True
    scraper_state['start_time'] = datetime.now().isoformat()
    scraper_state['error'] = None
    
    try:
        orchestrator = orchestrator_class(
            config_path=config_path,
            mongo_uri=mongo_uri,
            headless=True
        )
        report = orchestrator.run()
        
        scraper_state['end_time'] = datetime.now().isoformat()
        scraper_state['results'] = report
        scraper_state['alerts_count'] = report.get('total_alerts', 0)
        
        return report
        
    except Exception as e:
        logger.error(f"Error durante scraping: {e}")
        scraper_state['error'] = str(e)
        scraper_state['end_time'] = datetime.now().isoformat()
        raise
    finally:
        scraper_state['is_running'] = False


def run_scraper_background(
    orchestrator_class, 
    config_path: str, 
    mongo_uri: str, 
    scraper_state: Dict,
    results_collection
):
    """
    Ejecutar scraper y guardar resultados en MongoDB
    
    Args:
        orchestrator_class: Clase ScraperOrchestrator
        config_path: Ruta al archivo de configuración
        mongo_uri: URI de MongoDB
        scraper_state: Diccionario compartido con el estado del scraper
        results_collection: Colección de MongoDB para guardar resultados
    """
    try:
        report = run_scraper(
            orchestrator_class,
            config_path=config_path,
            mongo_uri=mongo_uri,
            scraper_state=scraper_state
        )
        
        # Guardar resultado en MongoDB
        report['saved_at'] = datetime.now()
        results_collection.insert_one(report)
        
    except Exception as e:
        logger.error(f"Error en background scraping: {e}")


def get_scraper_status(scraper_state: Dict) -> Dict:
    """
    Obtener el estado actual del scraper
    
    Args:
        scraper_state: Diccionario con el estado del scraper
    
    Returns:
        Diccionario con información del estado
    """
    return {
        'is_running': scraper_state['is_running'],
        'status': 'running' if scraper_state['is_running'] else 'idle',
        'start_time': scraper_state['start_time'],
        'end_time': scraper_state['end_time'],
        'alerts_count': scraper_state['alerts_count'],
        'error': scraper_state['error'],
        'results': scraper_state['results']
    }
