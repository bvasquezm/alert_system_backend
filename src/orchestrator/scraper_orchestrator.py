"""
Orquestador de scrapers para ejecutar jobs en paralelo por cada país
Utiliza ThreadPoolExecutor para ejecutar múltiples scrapers de forma concurrente
Guarda alertas en MongoDB
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.scraper import ComponentScraper
from src.storage import AlertStorage
import json
from datetime import datetime
from typing import Dict, List
import os
from dotenv import load_dotenv
import logging

load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

countries_backlist = ['AR'] # Estos países están en la blacklist por incompatibilidades con el scraper actual - se agregarán en el futuro


class ScraperOrchestrator:
    """Orquestador de scrapers para ejecución paralela por país"""
    
    def __init__(self, config_path: str = "config_components.json", headless: bool = True, max_workers: int = None, mongo_uri: str = None):
        """
        Inicializa el orquestador

        """
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        
        self.config_path = config_path
        self.headless = headless
        self.max_workers = max_workers or len(self.config)
        self.results = []
        self.alerts = []
        
        # Inicializar storage (MongoDB)
        self.mongo_uri = mongo_uri or os.getenv('MONGO_URI', 'mongodb://admin:password@mongodb:27017/scraper_alerts')
        try:
            print(f"  🔗 Conectando a MongoDB: {self.mongo_uri}")
            self.storage = AlertStorage(mongo_uri=self.mongo_uri)
            self.storage_available = True
            print(f"  ✅ Storage MongoDB listo")
        except Exception as e:
            logger.warning(f"No se pudo conectar a MongoDB: {e}. Las alertas se guardarán solo en memoria.")
            print(f"  ❌ MongoDB NO disponible: {e}")
            self.storage = None
            self.storage_available = False
        
        print(f"\n{'='*70}")
        print("INICIALIZANDO ORQUESTADOR DE SCRAPERS")
        print(f"{'='*70}")
        print(f"  📍 Países configurados: {', '.join(self.config.keys())}")
        print(f"  🔄 Workers paralelos: {self.max_workers}")
        print(f"  🎬 Modo headless: {'✓ Sí' if self.headless else '✗ No'}")
        print(f"  💾 Storage: {'MongoDB ✅' if self.storage_available else 'En memoria ⚠️'}")
        print(f"{'='*70}\n")
    
    def _print_summary(self, report: Dict) -> None:
        print(f"\n{'='*70}")
        print("RESUMEN DE EJECUCIÓN")
        print(f"{'='*70}")
        print(f"  ⏱️  Tiempo total: {report['execution_time']}")
        print(f"  ✅ Países exitosos: {report['successful']}/{report['total_countries']}")
        print(f"  ❌ Países fallidos: {report['failed']}/{report['total_countries']}")
        print(f"  ⚠️  Alertas generadas: {report['total_alerts']}")
        if self.storage_available:
            print(f"  💾 Alertas guardadas en MongoDB: {report['total_alerts']}")
            try:
                alerts_in_db = self.storage.load_alerts()
                print(f"  🔍 Alertas en BD: {len(alerts_in_db)}")
            except Exception:
                pass
        print(f"{'='*70}\n")

    def _scrape_country(self, country: str) -> Dict:
        """
        Ejecuta el scraping para un país específico
        
        Args:
            country: Código de país a scrapear
            
        Returns:
            Diccionario con resultados del scraping
        """
        print(f"\n▶️  Iniciando job para {country.upper()}")
        
        try:
            if country in countries_backlist:
                raise NotImplementedError(f"Scraping para {country} está en backlist - no implementado")

            scraper = ComponentScraper(self.config_path)
            scraper.headless = self.headless
            
            country_config = self.config.get(country, {})
            results_by_page = []
            country_alerts = []
            
            for page_type, page_config in country_config.items():
                if page_type == 'setup_product_url':
                    continue  # Saltar configuración especial
                
                url = page_config.get('url_example')
                if not url:
                    logger.warning(f"No se encontró URL para {country}/{page_type}")
                    continue
                
                print(f"  📄 Scrapeando {page_type}...")
                
                try:
                    result = scraper.scrape_page(
                        country,
                        page_type,
                        url
                    )
                    results_by_page.append(result)
                    
                    if result and 'error' not in result:
                        print(f"    ℹ️  Procesando resultado del scraping...")
                        print(f"    ℹ️  Keys en resultado: {result.keys()}")
                        
                        try:
                            page_alerts = scraper.generate_alerts(result)
                            print(f"    ℹ️  generate_alerts() retornó: {type(page_alerts)}, longitud: {len(page_alerts) if page_alerts else 0}")
                            
                            if page_alerts and len(page_alerts) > 0:
                                print(f"    📍 Alertas generadas: {len(page_alerts)}")
                                for alert in page_alerts:
                                    if 'status' not in alert:
                                        alert['status'] = 'MISSING_COMPONENT'
                                    if 'timestamp' not in alert:
                                        alert['timestamp'] = datetime.now().isoformat()
                                    country_alerts.append(alert)
                                    print(f"      ➕ Alerta: {alert.get('component')} - {alert.get('message')}")
                            else:
                                print(f"    ✅ No hay alertas para esta página (todo OK)")
                        except Exception as e:
                            print(f"    ❌ Error llamando generate_alerts(): {e}")
                            import traceback
                            traceback.print_exc()
                    else:
                        if 'error' in result:
                            print(f"    ❌ Error en scraping: {result['error']}")
                        else:
                            print(f"    ⚠️  No se pudo procesar el resultado")
                    
                except Exception as e:
                    logger.error(f"Error scrapeando {page_type}: {e}")
                    print(f"    ❌ Error: {e}")
            
            print(f"\n  📊 RESUMEN {country.upper()}:")
            print(f"     • Total alertas generadas: {len(country_alerts)}")
            print(f"     • Storage disponible: {'SÍ ✅' if self.storage_available else 'NO ❌'}")
            
            # GUARDADO DE ALERTAS EN MONGODB SI ESTÁ DISPONIBLE
            if len(country_alerts) > 0:
                if self.storage_available:
                    try:
                        print(f"     📤 Guardando {len(country_alerts)} alertas...")
                        self.storage.add_alerts(country_alerts)
                        print(f"     ✅ Guardadas exitosamente en MongoDB")
                    except Exception as e:
                        logger.error(f"Error guardando alertas en MongoDB: {e}")
                        print(f"     ❌ ERROR al guardar: {e}")
                else:
                    print(f"     ⚠️  MongoDB no disponible - alertas NO se guardarán")
            else:
                print(f"     ℹ️  Sin alertas para guardar")
            
            self.alerts.extend(country_alerts)

            scraper._close_browser()
            
            print(f"✅ Job completado para {country.upper()}")
            
            return {
                'country': country,
                'status': 'success',
                'alerts_count': len(country_alerts),
                'pages': results_by_page,
                'timestamp': datetime.now().isoformat()
            }

        except NotImplementedError as nie:
            logger.warning(str(nie))
            print(f"⚠️  {nie}")
            return {
                'country': country,
                'status': 'skipped',
                'error': str(nie),
                'timestamp': datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Error en job para {country.upper()}: {e}")
            print(f"❌ Error en job para {country.upper()}: {e}")
            return {
                'country': country,
                'status': 'failed',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def run(self) -> Dict:
        """
        Ejecuta todos los jobs de scraping en paralelo
        
        Returns:
            Diccionario con resultados agregados de todos los países
        """
        print(f"\n{'='*70}")
        print("EJECUTANDO JOBS EN PARALELO")
        print(f"{'='*70}\n")
        
        start_time = datetime.now()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            promise_results = {
                executor.submit(self._scrape_country, country): country 
                for country in self.config.keys()
            }
            
            for p_result in as_completed(promise_results):
                country = promise_results[p_result]
                try:
                    result = p_result.result()
                    self.results.append(result)
                except Exception as e:
                    logger.error(f"Excepción en job para {country}: {e}")
                    self.results.append({
                        'country': country,
                        'status': 'failed',
                        'error': str(e),
                        'timestamp': datetime.now().isoformat()
                    })
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        minutes, seconds = divmod(duration, 60)
        
        report = {
            'execution_time': f"{int(minutes)}m {int(seconds)}s",
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'total_countries': len(self.config),
            'successful': sum(1 for r in self.results if r['status'] == 'success'),
            'failed': sum(1 for r in self.results if r['status'] == 'failed'),
            'total_alerts': len(self.alerts),
            'results': self.results
        }
        
        self._print_summary(report)
        
        return report


if __name__ == "__main__":
    orchestrator = ScraperOrchestrator(
        config_path="config_components.json",
        headless=True,  # Ejecutar en modo headless
        max_workers=None  # Automático (uno por país)
    )
    
    # Ejecutar todos los jobs en paralelo
    report = orchestrator.run()
