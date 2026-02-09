"""
Servicio para gestionar alertas
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


def build_alerts_filter(
    country: Optional[str] = None,
    page_type: Optional[str] = None,
    status: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict:
    """
    Construir filtro de consulta para alertas
    
    Args:
        country: Código de país
        page_type: Tipo de página
        status: Estado de la alerta
        start_date: Fecha de inicio (ISO format)
        end_date: Fecha de término (ISO format)
    
    Returns:
        Diccionario con filtros para MongoDB
    """
    filter_query = {}
    
    if country:
        filter_query['country'] = country
    if page_type:
        filter_query['page_type'] = page_type
    if status:
        filter_query['status'] = status
    
    # Filtro por rango de fechas
    if start_date or end_date:
        date_filter = {}
        if start_date:
            try:
                start = datetime.fromisoformat(start_date)
                date_filter['$gte'] = start
                logger.info(f"Filtrando desde: {start.isoformat()}")
            except ValueError:
                logger.warning(f"Valor inválido para start_date: {start_date}")
        if end_date:
            try:
                end = datetime.fromisoformat(end_date)
                # Sumar un día para incluir todo el día final
                end = end + timedelta(days=1)
                date_filter['$lt'] = end
                logger.info(f"Filtrando hasta: {end.isoformat()}")
            except ValueError:
                logger.warning(f"Valor inválido para end_date: {end_date}")
        
        if date_filter:
            filter_query['timestamp'] = date_filter
    
    return filter_query


def get_alerts(
    alerts_collection,
    filter_query: Dict,
    page: int = 1,
    limit: int = 50
) -> Dict:
    """
    Obtener alertas con paginación
    
    Args:
        alerts_collection: Colección de MongoDB
        filter_query: Filtros a aplicar
        page: Número de página
        limit: Límite por página
    
    Returns:
        Diccionario con alertas y metadata de paginación
    """
    total = alerts_collection.count_documents(filter_query)
    logger.info(f"Total documentos encontrados: {total}")
    
    skip = (page - 1) * limit
    
    try:
        alerts = list(
            alerts_collection.find(filter_query)
            .sort('timestamp', -1)
            .skip(skip)
            .limit(limit)
        )
    except Exception as sort_error:
        logger.warning(f"Error ordenando por timestamp: {sort_error}, intentando por _id")
        alerts = list(
            alerts_collection.find(filter_query)
            .sort('_id', -1)
            .skip(skip)
            .limit(limit)
        )
    
    logger.info(f"Alertas recuperadas: {len(alerts)}")
    
    # Convertir ObjectId y datetime a string
    for alert in alerts:
        if '_id' in alert:
            alert['_id'] = str(alert['_id'])
        for key in ['date', 'timestamp']:
            if key in alert and hasattr(alert[key], 'isoformat'):
                alert[key] = alert[key].isoformat()
    
    pages = max(1, (total + limit - 1) // limit)
    
    return {
        'total': total,
        'page': page,
        'limit': limit,
        'pages': pages,
        'alerts': alerts
    }


def get_alerts_stats(alerts_collection) -> Dict:
    """
    Obtener estadísticas agregadas de alertas
    
    Args:
        alerts_collection: Colección de MongoDB
    
    Returns:
        Diccionario con estadísticas por país, tipo de página y estado
    """
    from bson.json_util import dumps, loads
    
    total_alerts = alerts_collection.count_documents({})
    
    # Alertas por país
    alerts_by_country = list(
        alerts_collection.aggregate([
            {'$group': {'_id': '$country', 'count': {'$sum': 1}}}
        ])
    )
    
    # Alertas por tipo de página
    alerts_by_page_type = list(
        alerts_collection.aggregate([
            {'$group': {'_id': '$page_type', 'count': {'$sum': 1}}}
        ])
    )
    
    # Alertas por status
    alerts_by_status = list(
        alerts_collection.aggregate([
            {'$group': {'_id': '$status', 'count': {'$sum': 1}}}
        ])
    )
    
    return {
        'total': total_alerts,
        'by_country': loads(dumps(alerts_by_country)),
        'by_page_type': loads(dumps(alerts_by_page_type)),
        'by_status': loads(dumps(alerts_by_status))
    }


def delete_all_alerts(alerts_collection) -> int:
    """
    Eliminar todas las alertas
    
    Args:
        alerts_collection: Colección de MongoDB
    
    Returns:
        Número de documentos eliminados
    """
    result = alerts_collection.delete_many({})
    return result.deleted_count


def get_alert_by_id(alerts_collection, alert_id: str) -> Optional[Dict]:
    """
    Obtener una alerta específica por ID
    
    Args:
        alerts_collection: Colección de MongoDB
        alert_id: ID de la alerta
    
    Returns:
        Diccionario con la alerta o None si no existe
    """
    from bson.objectid import ObjectId
    from bson.json_util import dumps, loads
    
    alert = alerts_collection.find_one({'_id': ObjectId(alert_id)})
    
    if not alert:
        return None
    
    alert['_id'] = str(alert['_id'])
    return loads(dumps(alert))
