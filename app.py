"""
Backend Flask para servir alertas desde MongoDB
"""
from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient
import os
import threading
from dotenv import load_dotenv
import logging
import urllib.error
import certifi

from orchestrator import ScraperOrchestrator
from services import alerts_service, scraper_service, report_service, teams_service

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, 
            template_folder='web',
            static_folder='web/static')
CORS(app)

MONGO_URI = os.getenv('MONGO_URI')

# Configurar TLS con CA bundle confiable para evitar errores de handshake
mongo_client_kwargs = {
    'serverSelectionTimeoutMS': int(os.getenv('MONGO_SERVER_SELECTION_TIMEOUT_MS', '30000')),
    'connectTimeoutMS': int(os.getenv('MONGO_CONNECT_TIMEOUT_MS', '20000')),
    'socketTimeoutMS': int(os.getenv('MONGO_SOCKET_TIMEOUT_MS', '20000')),
}

try:
    mongo_client_kwargs.update({
        'tls': True,
        'tlsCAFile': certifi.where(),
    })
except Exception as e:
    logger.warning(f"No se pudo configurar certifi: {type(e).__name__}: {str(e)}")

client = MongoClient(MONGO_URI, **mongo_client_kwargs)
db = client['scraper_alerts']
alerts_collection = db['alerts']
results_collection = db['results']

scraper_state = {
    'is_running': False,
    'start_time': None,
    'end_time': None,
    'results': None,
    'alerts_count': 0,
    'error': None
}


@app.route('/api/health', methods=['GET'])
def health():
    """Verificar salud del backend"""
    try:
        client.admin.command('ping')
        return jsonify({
            'status': 'healthy',
            'timestamp': scraper_service.get_scraper_status(scraper_state)['start_time'] or 'N/A',
            'mongodb': 'connected'
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500


@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    """Obtener todas las alertas con filtros"""
    try:
        country = request.args.get('country')
        page_type = request.args.get('page_type')
        status = request.args.get('status')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 50))
        
        logger.info(f"GET /api/alerts - Params: country={country}, page_type={page_type}, "
                   f"status={status}, start_date={start_date}, end_date={end_date}, page={page}, limit={limit}")
        
        filter_query = alerts_service.build_alerts_filter(
            country=country,
            page_type=page_type,
            status=status,
            start_date=start_date,
            end_date=end_date
        )
        
        result = alerts_service.get_alerts(
            alerts_collection,
            filter_query,
            page=page,
            limit=limit
        )
        
        logger.info(f"Retornando: Total={result['total']}, Page={result['page']}/{result['pages']}, "
                   f"Alertas={len(result['alerts'])}")
        
        return jsonify(result), 200
        
    except Exception as e:
        logger.error(f"ERROR en /api/alerts: {type(e).__name__}: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'error': str(e),
            'type': type(e).__name__
        }), 500


@app.route('/api/alerts/stats', methods=['GET'])
def get_alerts_stats():
    """Obtener estadísticas de alertas"""
    try:
        stats = alerts_service.get_alerts_stats(alerts_collection)
        return jsonify(stats), 200
    except Exception as e:
        logger.error(f"ERROR en /api/alerts/stats: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scrape', methods=['POST'])
def trigger_scrape():
    """Disparar un scraping manual"""
    try:
        if scraper_state['is_running']:
            return jsonify({
                'message': 'Scraping ya está en progreso',
                'status': 'already_running'
            }), 409
        
        # Ejecutar en thread separado
        thread = threading.Thread(
            target=scraper_service.run_scraper_background,
            args=(
                ScraperOrchestrator,
                'config_components.json',
                MONGO_URI,
                scraper_state,
                results_collection
            ),
            daemon=True
        )
        thread.start()
        
        return jsonify({
            'message': 'Scraping iniciado en background',
            'status': 'processing',
            'start_time': scraper_state['start_time']
        }), 202
        
    except Exception as e:
        logger.error(f"ERROR en /api/scrape: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/scrape/status', methods=['GET'])
def scrape_status():
    """Obtener estado del último scraping"""
    try:
        status = scraper_service.get_scraper_status(scraper_state)
        return jsonify(status), 200
    except Exception as e:
        logger.error(f"ERROR en /api/scrape/status: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts/<alert_id>', methods=['GET'])
def get_alert(alert_id):
    """Obtener una alerta específica por ID"""
    try:
        alert = alerts_service.get_alert_by_id(alerts_collection, alert_id)
        
        if not alert:
            return jsonify({'error': 'Alert not found'}), 404
        
        return jsonify(alert), 200
    except Exception as e:
        logger.error(f"ERROR en /api/alerts/{alert_id}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/alerts', methods=['DELETE'])
def delete_alerts():
    """Limpiar todas las alertas"""
    try:
        deleted_count = alerts_service.delete_all_alerts(alerts_collection)
        return jsonify({'deleted': deleted_count}), 200
    except Exception as e:
        logger.error(f"ERROR en DELETE /api/alerts: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/debug', methods=['GET'])
def debug_info():
    """Endpoint de debug - información de la BD"""
    try:
        from bson.json_util import dumps, loads
        
        alerts_count = alerts_collection.count_documents({})
        
        by_country = list(alerts_collection.aggregate([
            {'$group': {'_id': '$country', 'count': {'$sum': 1}}}
        ]))
        
        latest_alerts = list(alerts_collection.find().sort('timestamp', -1).limit(5))
        
        return jsonify({
            'total_alerts_in_db': alerts_count,
            'by_country': by_country,
            'latest_5_alerts': loads(dumps(latest_alerts))
        }), 200
    except Exception as e:
        logger.error(f"ERROR en /api/debug: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/report', methods=['GET'])
def alerts_report():
    """Obtener el último reporte de scraping"""
    try:
        latest_report = report_service.get_latest_report(results_collection)
        
        if not latest_report:
            return jsonify({
                'error': 'No se encontraron reportes',
                'message': 'Aún no hay reportes disponibles'
            }), 404
        
        logger.info(f"Reporte encontrado: {latest_report.get('_id')}")
        return jsonify(latest_report), 200
        
    except Exception as e:
        logger.error(f"ERROR en /api/report: {type(e).__name__}: {str(e)}")
        return jsonify({
            'error': str(e),
            'type': type(e).__name__
        }), 500


@app.route('/api/teams/report', methods=['POST'])
def teams_report_send():
    """Generar y enviar reporte de alertas a Teams"""
    try:
        webhook_url = os.getenv('TEAMS_WEBHOOK_URL')
        
        result = teams_service.generate_and_send_teams_report(
            results_collection,
            webhook_url
        )
        
        if result['status'] == 'sent':
            return jsonify({'status': 'sent'}), 200
        else:
            return jsonify({
                'error': 'Teams webhook error',
                'status': result['status_code']
            }), 502
    
    except ValueError as e:
        logger.error(f"ValueError en /api/teams/report: {str(e)}")
        if 'not configured' in str(e):
            return jsonify({'error': str(e)}), 500
        else:
            return jsonify({
                'error': str(e),
                'message': 'Aún no hay reportes disponibles'
            }), 404
    
    except urllib.error.HTTPError as e:
        logger.error(f"Teams webhook HTTPError: {e.code}")
        return jsonify({
            'error': 'Teams webhook HTTPError',
            'status': e.code
        }), 502
    
    except Exception as e:
        logger.error(f"ERROR en /api/teams/report: {type(e).__name__}: {str(e)}")
        return jsonify({
            'error': str(e),
            'type': type(e).__name__
        }), 500


if __name__ == '__main__':
    logger.info("="*70)
    logger.info("🚀 INICIANDO BACKEND FLASK")
    logger.info("="*70)
    logger.info(f"📍 MongoDB URI: {MONGO_URI}")
    logger.info(f"🎯 Host: 0.0.0.0:5000")
    logger.info(f"📦 CORS habilitado")
    logger.info("="*70)
    
    try:
        client.admin.command('ping')
        logger.info("✅ Conexión a MongoDB exitosa")
    except Exception as e:
        logger.error(f"⚠️  No se pudo conectar a MongoDB: {e}")
    
    app.run(host='0.0.0.0', port=5000, debug=False)
