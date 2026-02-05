"""
Servicio para generar y enviar reportes
"""
from datetime import datetime, timedelta
from typing import Dict, Optional
import logging
from bson.json_util import dumps, loads

logger = logging.getLogger(__name__)


def get_latest_report(results_collection) -> Optional[Dict]:
    """
    Obtener el último reporte de scraping
    
    Args:
        results_collection: Colección de MongoDB
    
    Returns:
        Diccionario con el reporte o None si no existe
    """
    latest_result = results_collection.find_one({}, sort=[('saved_at', -1)])
    
    if not latest_result:
        return None
    
    latest_result['_id'] = str(latest_result['_id'])
    latest_result['results'] = [
        {
            "country": country["country"],
            "total_alerts": country["alerts_count"],
            "status": country["status"],
            "timestamp": country["timestamp"]
        } for country in latest_result.get('results', [])
    ]
    
    return loads(dumps(latest_result))


def filter_results_by_time(results: list, hours: int = 24) -> list:
    """
    Filtrar resultados por tiempo (últimas N horas)
    
    Args:
        results: Lista de resultados del reporte
        hours: Número de horas hacia atrás
    
    Returns:
        Lista filtrada de resultados
    """
    cutoff = datetime.now() - timedelta(hours=hours)
    filtered_results = []
    
    for alert in results:
        timestamp = alert.get('timestamp')
        if hasattr(timestamp, 'isoformat'):
            timestamp = timestamp.isoformat()
        if isinstance(timestamp, str):
            try:
                parsed_ts = datetime.fromisoformat(timestamp)
            except ValueError:
                parsed_ts = None
        elif isinstance(timestamp, datetime):
            parsed_ts = timestamp
        else:
            parsed_ts = None
        
        if parsed_ts and parsed_ts >= cutoff:
            filtered_results.append(alert)
    
    return filtered_results
