# Scraper Components Backend

Backend Flask con endpoints para:

- `/api/health`: Healthcheck y verificación de conexión a MongoDB
- `/api/alerts` y `/api/alerts/stats`: Lectura y estadísticas de alertas con filtros/paginación
- `/api/scrape` y `/api/scrape/status`: Disparar scraping y consultar estado
- `/api/report` y `/api/teams/report`: Último reporte y envío a Microsoft Teams

## Estructura del Proyecto

```
/
├── app.py                      # Aplicación Flask principal
├── run_scrape_and_report.py    # Script CLI para ejecutar scraping
├── config_components.json      # Configuración de componentes a scrapear
├── requirements.txt            # Dependencias Python
├── vercel.json                # Configuración de Vercel
└── src/                       # Código fuente principal
    ├── orchestrator/          # Orquestador de scraping paralelo
    │   ├── __init__.py
    │   └── scraper_orchestrator.py
    ├── scraper/              # Lógica de scraping con Playwright
    │   ├── __init__.py
    │   └── component_scraper.py
    ├── storage/              # Operaciones de base de datos
    │   ├── __init__.py
    │   └── alert_storage.py
    └── services/             # Servicios de aplicación
        ├── __init__.py
        ├── alerts_service.py
        ├── report_service.py
        ├── scraper_service.py
        └── teams_service.py
```

## Requisitos

- Python 3.11+
- MongoDB (recomendado Atlas M0). Variable `MONGO_URI` con `authSource=admin` cuando corresponda
- Playwright 1.40.0 (se instalará con `requirements.txt`). Para ejecutar browsers localmente: `python -m playwright install --with-deps chromium`

## Variables de entorno

- `MONGO_URI` (requerida): ej. `mongodb+srv://user:pass@cluster/db?retryWrites=true&w=majority&authSource=admin`
- `PLAYWRIGHT_HEADLESS` (opcional): `true`/`false` (default `true`)
- `PLAYWRIGHT_TIMEOUT_MS` (opcional): tiempo de navegación, default `30000`
- `TEAMS_WEBHOOK_URL` (opcional): para `/api/teams/report`

## Desarrollo local

```bash
pip install -r requirements.txt
python app.py
```

La UI de ejemplo (estática) está en el repo original; este backend no sirve `web/` en este split.

## Producción (ideas)

- Docker con base `mcr.microsoft.com/playwright/python:v1.40.0-jammy`
- Fly.io / EC2 free tier para API 24/7
- GitHub Actions para job diario llamando `run_scrape_and_report.py`