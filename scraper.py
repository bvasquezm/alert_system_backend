"""
Scraper de componentes web para detección de alertas
Utiliza Playwright para renderizar JavaScript y BeautifulSoup para extraer componentes
"""
from playwright.sync_api import sync_playwright, Page
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Tuple
import unicodedata
import json
from datetime import datetime
import time
import os


class ComponentScraper:
    def __init__(self, config_path: str = "config_components.json"):
        """
        Inicializa el scraper con la configuración de componentes
        """
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = json.load(f)
        
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        self.needs_to_setup = True
        self.setup_completed = False
        # En contenedores, preferir headless; configurable por env PLAYWRIGHT_HEADLESS (true/false)
        self.headless = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"
        # Timeout de navegación configurable (ms)
        try:
            self.default_timeout_ms = int(os.getenv("PLAYWRIGHT_TIMEOUT_MS", "30000"))
        except ValueError:
            self.default_timeout_ms = 30000
        self.scroll_speed = 60

    # Utilidad para normalizar texto y hacer match parcial robusto
    def _normalize_text(self, text: str) -> str:
        normalized = unicodedata.normalize('NFKD', text)
        return normalized.encode('ascii', 'ignore').decode('ascii').lower()

    def _partial_match(self, text: str, pattern: str) -> bool:
        return self._normalize_text(pattern) in self._normalize_text(text)
    
    def _init_browser(self):
        """Inicializa el browser de Playwright si no está iniciado"""
        if not self.playwright:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(
                headless=self.headless,
                args=[
                    "--no-sandbox",
                    "--ignore-certificate-errors",
                    "--disable-dev-shm-usage"
                ]
            )
            # Ignorar errores HTTPS por certificados no confiables
            self.context = self.browser.new_context(ignore_https_errors=True)
            # Establecer timeouts por defecto
            self.context.set_default_navigation_timeout(self.default_timeout_ms)
            self.context.set_default_timeout(self.default_timeout_ms)
            self.page = self.context.new_page()
            print("  🌐 Browser iniciado")
    
    def _close_browser(self):
        """Cierra el browser de Playwright"""
        if self.page:
            self.page.close()
            self.page = None
        if self.context:
            self.context.close()
            self.context = None
        if self.browser:
            self.browser.close()
            self.browser = None
        if self.playwright:
            self.playwright.stop()
            self.playwright = None
        print("  🔒 Browser cerrado")
    
    def setup_navigation(self, country: str):
        """ Realiza navegación previa y agrega producto al carro. 
            Mantiene la sesión para el scraping posterior
        """

        if not self.needs_to_setup:
            print(f"  ✖️ No se realizará setup de navegación")
            return
        
        elif self.setup_completed:
            print(f"  🆗 Setup de navegación ya completado")
            return
        
        try:
            print(f"\n{'='*60}")
            print("EJECUTANDO SETUP DE NAVEGACIÓN")
            print(f"{'='*60}")
            
            self._init_browser()
            
            # Obtener URL del producto de setup
            setup_url = self.config.get(country, {}).get('setup_product_url')
            if not setup_url:
                print(f"  ⚠️  No se encontró setup_product_url para {country}")
                return
            
            print(f"  📦 Navegando a producto de setup: {setup_url}")
            try:
                self.page.goto(setup_url, wait_until='domcontentloaded', timeout=5000)
            except Exception as e:
                print(f"  ❌ Error navegando al producto de setup: {e}")
                return
            
            print(f"  🎯 Buscando popover previo...")
            try:
                popover_container = self.page.query_selector('[data-testid="coachmark-popover"]')
                if popover_container:
                    popover_button = popover_container.query_selector('[data-testid="popover-button"]')
                    if popover_button:
                        print(f"  ✓ Popover encontrado, haciendo click...")
                        popover_button.click()
                        time.sleep(1)
                    else:
                        print(f"  ⚠️  Botón popover-button no encontrado dentro del popover")
                else:
                    print(f"  ⚠️  Popover no disponible")
            except Exception as e:
                print(f"  ⚠️  Popover no disponible o error al clickear: {e}")
            
            print(f"  🛒 Buscando botón 'Agregar al carro'...")
            try:
                add_button = self.page.wait_for_selector('button#add-to-cart-button, button#testId-btn-add-to-cart', state='visible', timeout=6000)
                print(f"  ✓ Botón encontrado, haciendo click...")
                add_button.click()
                print(f"  ✓ Click realizado")
                
                print(f"  ⏳ Esperando confirmación...")
                time.sleep(3)
                print(f"  ✓ Producto agregado al carro")
                
            except Exception as e:
                print(f"  ⚠️  Error al agregar al carro: {e}")
            
            self.setup_completed = True
            print(f"  ✓ Setup completado, sesión lista para scraping")
            print(f"{'='*60}\n")
            
        except Exception as e:
            print(f"  ❌ Error en setup de navegación: {e}")
    
    def fetch_page(self, url: str, wait_time: int = 5, country: str = None, page_type: str = None) -> Optional[BeautifulSoup]:
        try:
            print(f"  📡 Obteniendo página con Playwright...")
            
            # Verificar si necesita setup: por página o por tener setup_product_url definido
            needs_setup = False
            if country and page_type:
                page_config = self.config.get(country, {}).get(page_type, {})
                needs_setup = page_config.get('setup_required', False)

            has_country_setup = bool(self.config.get(country, {}).get('setup_product_url'))

            if (needs_setup or has_country_setup) and not self.setup_completed:
                self.setup_navigation(country)
            
            self._init_browser()
            
            print(f"  ⏳ Navegando a {url}...")
            self.page.goto(url, wait_until='domcontentloaded', timeout=self.default_timeout_ms)
            
            print(f"  📜 Scrolleando para disparar lazy loading...")
            self._scroll_page(self.page)
            
            content = self.page.content()
            
            return BeautifulSoup(content, 'html.parser')
            
        except Exception as e:
            print(f"❌ Error al obtener la página {url}: {e}")
            return None
    
    def _scroll_page(self, page: Page):
        """
        Scrollea la página para disparar lazy loading
        
        Args:
            page: Página de Playwright
        """
        try:
            page.evaluate(f"""
                async () => {{
                    await new Promise((resolve) => {{
                        let totalHeight = 0;
                        const distance = 100;
                        const timer = setInterval(() => {{
                            const scrollHeight = document.body.scrollHeight;
                            window.scrollBy(0, distance);
                            totalHeight += distance;
                            
                            if(totalHeight >= scrollHeight){{
                                clearInterval(timer);
                                resolve();
                            }}
                        }}, {self.scroll_speed});
                    }});
                }}
            """)
            
            # Esperar un poco más después del scroll
            time.sleep(1)
            
            # Volver al inicio
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(2)
        except Exception as e:
            print(f"  ⚠️  Error al scrollear: {e}")
    
    def find_component_by_data_testid(self, soup: BeautifulSoup, value: str) -> bool:
        """Busca un componente por data-testid (soporta data-testid y data-test-id)"""
        element = soup.find(attrs={"data-testid": value})
        if not element:
            element = soup.find(attrs={"data-test-id": value})
        return element is not None
    
    def find_component_by_class(self, soup: BeautifulSoup, value: str) -> bool:
        """Busca un componente por clase CSS"""
        elements = self.find_elements_by_class(soup, value)
        return len(elements) > 0

    def find_elements_by_class(self, soup: BeautifulSoup, value: str) -> List[BeautifulSoup]:
        """Retorna todos los elementos que contengan las clases dadas"""
        classes = value.split()
        return soup.find_all(class_=classes)
    
    def _check_component_by_identifier_type(self, soup: BeautifulSoup, component: Dict) -> Tuple[bool, Optional[List[BeautifulSoup]]]:
        identifier_type = component['identifier_type']
        identifier_value = component['identifier_value']
        
        if identifier_type in ('data-testid', 'data-test-id'):
            element = soup.find(attrs={str(identifier_type): identifier_value})        
            found = element is not None
            return (found, [element] if found else None)
        
        elif identifier_type == 'class':
            elements = self.find_elements_by_class(soup, identifier_value)
            return (len(elements) > 0, elements if len(elements) > 0 else None)
        
        elif identifier_type == 'id':
            found = self.find_component_by_id(soup, identifier_value)
            element = soup.find(id=identifier_value) if found else None
            return (found, [element] if found else None)
        
        return (False, None)
    
    def _build_component_details(self, component: Dict, elements: List[BeautifulSoup]) -> Optional[Dict]:
        if not elements:
            return None
        
        if 'carousel_strategies' in component:
            strategy_result = self._extract_strategies(elements, component['carousel_strategies'])
            return {
                'carousels': [el.get('id') or f"carousel-{idx+1}" for idx, el in enumerate(elements)],
                'strategies': strategy_result
            }
        
        elif 'text_strategies' in component:
            strategy_result = self._extract_strategies(elements, component['text_strategies'])
            return {
                'elements': [el.get('id') or f"el-{idx+1}" for idx, el in enumerate(elements)],
                'strategies': strategy_result
            }
        
        return None
    
    def _extract_strategies(self, elements: List[BeautifulSoup], strategies: List[Dict]) -> Dict:
        return self.find_strategies_in_elements(elements, strategies)

    def find_strategies_in_elements(self, elements: List[BeautifulSoup], strategies: List[Dict]) -> Dict:
        """Busca estrategias en elementos con match parcial normalizado."""
        strategies_found: Dict[str, bool] = {s['strategy_name']: False for s in strategies}
        strategies_details: Dict[str, Dict[str, List[str]]] = {s['strategy_name']: {'found_in': []} for s in strategies}
        potential_matches: Dict[str, List[str]] = {s['strategy_name']: [] for s in strategies}
        
        for idx, el in enumerate(elements):
            label = el.get('id') or f"el-{idx+1}"
            
            for strat in strategies:
                name = strat['strategy_name']
                if strategies_found[name]:
                    continue
                
                container_class = strat.get('container_class')
                if container_class:
                    search_in = el.find_all(class_=container_class)
                else:
                    search_in = [el]
                
                for container_idx, container in enumerate(search_in):
                    container_text = container.get_text(strip=True)
                    normalized_container = self._normalize_text(container_text)
                    normalized_pattern = self._normalize_text(strat['text_pattern'])
                    
                    if normalized_pattern in normalized_container:
                        strategies_found[name] = True
                        strategies_details[name]['found_in'].append(label)
                        break
                    else:
                        potential_matches[name].append(container_text)

        for name, found in strategies_found.items():
            if not found:
                print(f"    ✗ Estrategia '{name}' no encontrada")

        return {
            'strategies_found': strategies_found,
            'strategies_details': strategies_details,
            'potential_matches': potential_matches
        }
    
    def check_component(self, soup: BeautifulSoup, component: Dict) -> Dict:
        """ Verifica si un componente existe en la página. """

        result = {
            'name': component['name'],
            'found': False,
            'details': None
        }
        
        # Buscar componente según tipo
        found, elements = self._check_component_by_identifier_type(soup, component)
        result['found'] = found
        
        # Si se encontró, extraer detalles (estrategias, etc.)
        if found:
            result['details'] = self._build_component_details(component, elements)
        else:
            self._log_component_not_found(component)
        
        return result
    
    def _log_component_not_found(self, component: Dict) -> None:
        """Loguea cuando un componente no se encuentra"""
        identifier_type = component['identifier_type']
        identifier_value = component['identifier_value']
        
        if identifier_type == 'id':
            print(f"    ⚠️  Elemento #{identifier_value} NO encontrado en el HTML")

    def scrape_page(self, country: str, page_type: str, url: str) -> Dict:
        """ Scrapea una página y verifica todos los componentes configurados. """

        print(f"\n{'='*60}")
        print(f"Scrapeando {country} - {page_type}")
        print(f"URL: {url}")
        print(f"{'='*60}")
        
        # Obtener configuración
        if country not in self.config:
            return {
                'error': f'País {country} no configurado',
                'timestamp': datetime.now().isoformat()
            }
        
        if page_type not in self.config[country]:
            return {
                'error': f'Tipo de página {page_type} no configurado para {country}',
                'timestamp': datetime.now().isoformat()
            }
        
        page_config = self.config[country][page_type]
        components = page_config['components']
        
        # HTML
        soup = self.fetch_page(url, country=country, page_type=page_type)
        if soup is None:
            return {
                'error': 'No se pudo obtener la página',
                'timestamp': datetime.now().isoformat()
            }
        
        results = []
        for component in components:
            result = self.check_component(soup, component)
            results.append(result)
            
            status = "✓ ENCONTRADO" if result['found'] else "✗ NO ENCONTRADO"
            print(f"{status}: {result['name']}")
            if result['details']:
                if 'strategies' in result['details']:
                    # Mostrar solo resumen de estrategias
                    strategies = result['details'].get('strategies', {})
                    strategies_found = strategies.get('strategies_found', {})
                    strategies_details = strategies.get('strategies_details', {})
                    
                    for strategy_name, found in strategies_found.items():
                        status_strat = "✓" if found else "✗"
                        found_in = strategies_details.get(strategy_name, {}).get('found_in', [])
                        if found:
                            where = f" (en {', '.join(found_in)})" if found_in else ""
                            print(f"    {status_strat} {strategy_name}{where}")
                        else:
                            print(f"    {status_strat} {strategy_name}: NO encontrada")
                else:
                    # Mostrar información de carruseles sin estrategias
                    matched_ids = result['details'].get('matched_ids', [])
                    if matched_ids:
                        print(f"  Elementos encontrados: {', '.join(matched_ids)}")
        
        return {
            'country': country,
            'page_type': page_type,
            'url': url,
            'timestamp': datetime.now().isoformat(),
            'components': results
        }
    
    def generate_alerts(self, scrape_results: Dict) -> List[Dict]:
        """
        Genera alertas para componentes no encontrados.
        """

        if 'error' in scrape_results:
            return self._create_error_alert(scrape_results)
        
        alerts = []
        page_type = scrape_results.get('page_type', '').upper()

        for component in scrape_results['components']:
            alerts.extend(self._create_missing_component_alert(component, scrape_results, page_type))
        
        return alerts
    
    def _create_error_alert(self, scrape_results: Dict) -> List[Dict]:
        """Crea alerta de error"""
        return [{
            'date': scrape_results['timestamp'],
            'country': scrape_results.get('country', 'Unknown'),
            'page_type': scrape_results.get('page_type', 'Unknown'),
            'component': 'N/A',
            'status': 'ERROR',
            'message': scrape_results['error']
        }]
    
    def _create_missing_component_alert(self, component: Dict, scrape_results: Dict, page_type: str) -> List[Dict]:
        """Crea alertas para componentes y estrategias faltantes"""
        alerts = []
        
        # Si el componente no fue encontrado
        if not component['found']:
            message = f"Componente '{component['name']}' no encontrado en {scrape_results['page_type']}"
            alerts.append({
                'date': scrape_results['timestamp'],
                'country': scrape_results['country'],
                'page_type': scrape_results['page_type'],
                'component': component['name'],
                'status': 'MISSING_COMPONENT',
                'message': message
            })
        
        elif component['details'] and 'strategies' in component['details']:
            strategies = component['details'].get('strategies', {})
            strategies_found = strategies.get('strategies_found', {})
            potential_matches = strategies.get('potential_matches', {})
            
            for strategy_name, found in strategies_found.items():
                if found:
                    continue
                candidates = potential_matches.get(strategy_name, []) if potential_matches else []
                has_candidates = len(candidates) > 0
                
                if has_candidates:
                    sample = "; ".join([c[:80] + ('...' if len(c) > 80 else '') for c in candidates[:3]])
                    message = (
                        f"Se encontraron títulos diferentes para '{strategy_name}': {sample}. "
                        f"Revisar posible cambio de nombre."
                    )
                else:
                    message = f"Estrategia '{strategy_name}' no encontrada en componente '{component['name']}'"
                
                alerts.append({
                    'date': scrape_results['timestamp'],
                    'country': scrape_results['country'],
                    'page_type': scrape_results['page_type'],
                    'component': f"{component['name']} - {strategy_name}",
                    'status': 'MISSING_COMPONENT',
                    'message': message
                })
        
        return alerts


if __name__ == "__main__":
    # Test básico
    scraper = ComponentScraper()
    print("Scraper inicializado correctamente")
    print(f"Países configurados: {list(scraper.config.keys())}")
