"""
Servicio para gestionar comunicación con Microsoft Teams
"""
from datetime import datetime
from typing import Dict
import json
import urllib.request
import urllib.error
import logging

logger = logging.getLogger(__name__)


def extract_components_issues(alert: Dict) -> Dict[str, set]:
    """
    Extraer componentes con conflictos de un alert
    
    Args:
        alert: Diccionario con información del alert
    
    Returns:
        Diccionario con componentes y páginas donde tienen conflictos
    """
    components_issues = {}
    pages = alert.get('pages', [])
    
    for page in pages:
        page_type = page.get('page_type', 'N/A')
        components = page.get('components', [])
        
        for component in components:
            component_name = component.get('name', 'N/A')
            component_found = component.get('found', True)
            
            # Si el componente no fue encontrado, agregarlo directamente
            if not component_found:
                if component_name not in components_issues:
                    components_issues[component_name] = set()
                components_issues[component_name].add(page_type)
            
            # Si el componente fue encontrado, verificar estrategias
            details = component.get('details')
            if details and isinstance(details, dict):
                strategies = details.get('strategies', {})
                strategies_found = strategies.get('strategies_found', {})
                
                for strategy_name, found in strategies_found.items():
                    if not found:
                        if strategy_name not in components_issues:
                            components_issues[strategy_name] = set()
                        components_issues[strategy_name].add(page_type)
    
    return components_issues


def generate_teams_message(filtered_results: list) -> str:
    """
    Generar mensaje formateado para Teams
    
    Args:
        filtered_results: Lista de resultados filtrados
    
    Returns:
        String con el mensaje formateado en HTML/Markdown para Teams
    """
    total_alerts = sum(r.get('alerts_count', 0) for r in filtered_results)
    
    if total_alerts == 0:
        return 'No hay alertas nuevas durante las últimas 24 horas.'
    
    today = datetime.now().strftime('%d/%m/%Y')
    message = f"**ALERTAS - ÚLTIMAS 24 HORAS [{today}]**<br><br>"
    message += f"**Total alertas: {total_alerts}**<br>"
    message += f"<br>---<br><br>"
    
    for alert in filtered_results:
        timestamp = alert.get('timestamp')
        if hasattr(timestamp, 'isoformat'):
            timestamp = timestamp.isoformat()
        if isinstance(timestamp, str):
            try:
                parsed_ts = datetime.fromisoformat(timestamp)
                timestamp = parsed_ts.strftime('%d/%m/%Y - %H:%M hrs')
            except ValueError:
                timestamp = 'N/A'
        
        message += (
            f"**País:** {alert.get('country')}<br>"
            f"**Estado:** {alert.get('status')}<br>"
            f"**Alertas:** {alert.get('alerts_count')}<br>"
            f"**Fecha/Hora:** {timestamp}<br>"
        )
        
        # Agregar componentes con conflictos si hay alertas
        if alert.get('alerts_count', 0) > 0:
            components_issues = extract_components_issues(alert)
            
            if components_issues:
                message += f"<br>**Componentes con conflictos:**<br>"
                for comp_name, page_types in components_issues.items():
                    message += f"- {comp_name}: {', '.join(sorted(page_types))}<br>"
        
        message += "<br>"
    
    return message


def send_to_teams_webhook(webhook_url: str, message: str) -> int:
    """
    Enviar mensaje al webhook de Teams
    
    Args:
        webhook_url: URL del webhook de Teams
        message: Mensaje a enviar
    
    Returns:
        Código de estado HTTP
    
    Raises:
        urllib.error.HTTPError: Si hay error en la petición HTTP
    """
    body = json.dumps({'text': message}).encode('utf-8')
    req = urllib.request.Request(
        webhook_url,
        data=body,
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.getcode()


def generate_and_send_teams_report(results_collection, webhook_url: str) -> Dict:
    """
    Generar y enviar reporte completo a Teams
    
    Args:
        results_collection: Colección de MongoDB con resultados
        webhook_url: URL del webhook de Teams
    
    Returns:
        Diccionario con el resultado de la operación
    
    Raises:
        ValueError: Si no hay webhook configurado o no hay reportes
        urllib.error.HTTPError: Si hay error al enviar a Teams
    """
    from services.report_service import filter_results_by_time
    
    if not webhook_url:
        raise ValueError('TEAMS_WEBHOOK_URL not configured')
    
    latest_result = results_collection.find_one({}, sort=[('saved_at', -1)])
    
    if not latest_result:
        raise ValueError('No se encontraron reportes')
    
    results = latest_result.get('results', []) or []
    filtered_results = filter_results_by_time(results, hours=24)
    message = generate_teams_message(filtered_results)
    status_code = send_to_teams_webhook(webhook_url, message)
    
    return {
        'status': 'sent' if 200 <= status_code < 300 else 'error',
        'status_code': status_code
    }
