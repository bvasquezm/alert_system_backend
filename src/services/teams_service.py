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
        Diccionario con componentes DISTINTOS y páginas donde tienen conflictos.
        Para componentes con estrategias (como Recommendation Carousels), solo reporta
        las estrategias faltantes individuales, no el componente padre.
    """
    components_issues = {}
    pages = alert.get('pages', [])
    
    for page in pages:
        page_type = page.get('page_type', 'N/A')
        components = page.get('components', [])
        
        for component in components:
            component_name = component.get('name', 'Unknown')
            component_found = component.get('found', False)
            details = component.get('details')
            
            # Verificar si hay estrategias con problemas
            has_strategies = (
                details and 
                isinstance(details, dict) and 
                'strategies' in details and 
                details.get('strategies') is not None
            )
            
            if has_strategies:
                # Si el componente tiene estrategias, solo reportar las estrategias faltantes
                strategies = details.get('strategies', {})
                strategies_found = strategies.get('strategies_found', {}) if strategies else {}
                
                # Agregar cada estrategia faltante como componente individual
                for strategy_name, found in strategies_found.items():
                    if not found:
                        if strategy_name not in components_issues:
                            components_issues[strategy_name] = set()
                        components_issues[strategy_name].add(page_type)
            
            # Si el componente no fue encontrado y NO tiene estrategias, agregarlo
            elif not component_found:
                if component_name not in components_issues:
                    components_issues[component_name] = set()
                components_issues[component_name].add(page_type)
    
    return components_issues


def generate_teams_message(filtered_results: list) -> str:
    """
    Generar mensaje formateado para Teams
    
    Args:
        filtered_results: Lista de resultados filtrados
    
    Returns:
        String con el mensaje formateado en HTML/Markdown para Teams
    """
    # Pre-calcular componentes distintos para cada país (evita cálculo duplicado)
    results_with_components = []
    total_distinct_components = 0
    
    for r in filtered_results:
        components_issues = extract_components_issues(r)
        if len(components_issues) > 0:
            results_with_components.append({
                'alert': r,
                'components_issues': components_issues,
                'distinct_count': len(components_issues)
            })
            total_distinct_components += len(components_issues)
    
    if total_distinct_components == 0:
        return 'No hay alertas nuevas durante las últimas 24 horas.'
    
    today = datetime.now().strftime('%d/%m/%Y')
    message = f"**ALERTAS - ÚLTIMAS 24 HORAS [{today}]**<br><br>"
    message += f"**Total alertas: {total_distinct_components}**<br>"
    message += f"<br>---<br><br>"
    
    for result in results_with_components:
        alert = result['alert']
        components_issues = result['components_issues']
        distinct_count = result['distinct_count']
        
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
            f"**Alertas:** {distinct_count}<br>"
            f"**Fecha/Hora:** {timestamp}<br>"
        )
        
        # Agregar componentes con conflictos
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
    from src.services.report_service import filter_results_by_time
    
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
