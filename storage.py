"""
Sistema de almacenamiento de alertas en MongoDB
"""
from pymongo import MongoClient
from datetime import datetime
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv

load_dotenv()


class AlertStorage:
    """Maneja el almacenamiento de alertas en MongoDB"""
    
    def __init__(self, mongo_uri: Optional[str] = None):
        """
        Inicializa el almacenamiento MongoDB
        
        Args:
            mongo_uri: URI de conexión a MongoDB (por defecto usa MONGO_URI del .env)
        """
        self.mongo_uri = mongo_uri or os.getenv('MONGO_URI', 'mongodb://localhost:27017')
        self.client = None
        self.db = None
        self.collection = None
        self._connect()
    
    def _connect(self):
        """Conecta a MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri)
            # Verificar conexión
            self.client.admin.command('ping')
            self.db = self.client['scraper_alerts']
            self.collection = self.db['alerts']
            print("  ✅ Conexión a MongoDB exitosa")
        except Exception as e:
            print(f"  ❌ Error al conectar a MongoDB: {e}")
            raise
    
    def load_alerts(self) -> List[Dict]:
        """
        Carga todas las alertas del almacenamiento
        
        Returns:
            Lista de alertas
        """
        try:
            alerts = list(self.collection.find().sort('date', -1))
            return alerts
        except Exception as e:
            print(f"  ❌ Error al cargar alertas: {e}")
            return []
    
    def save_alert(self, alert: Dict) -> str:
        """
        Guarda una alerta individual
        
        Args:
            alert: Alerta a guardar
            
        Returns:
            ID del documento insertado
        """
        try:
            result = self.collection.insert_one(alert)
            return str(result.inserted_id)
        except Exception as e:
            print(f"  ❌ Error al guardar alerta: {e}")
            return None
    
    def add_alerts(self, new_alerts: List[Dict]):
        """
        Agrega nuevas alertas al almacenamiento
        
        Args:
            new_alerts: Lista de nuevas alertas
        """
        try:
            if new_alerts:
                result = self.collection.insert_many(new_alerts)
                print(f"  ✅ {len(result.inserted_ids)} alertas guardadas en MongoDB")
        except Exception as e:
            print(f"  ❌ Error al agregar alertas: {e}")

    def get_all_alerts(self) -> List[Dict]:
        """Retorna todas las alertas almacenadas"""
        return self.load_alerts()
    
    def get_alerts_by_country(self, country: str) -> List[Dict]:
        """Filtra alertas por país"""
        try:
            alerts = list(self.collection.find({'country': country}).sort('date', -1))
            return alerts
        except Exception as e:
            print(f"  ❌ Error al filtrar por país: {e}")
            return []
    
    def get_alerts_by_page_type(self, page_type: str) -> List[Dict]:
        """Filtra alertas por tipo de página"""
        try:
            alerts = list(self.collection.find({'page_type': page_type}).sort('date', -1))
            return alerts
        except Exception as e:
            print(f"  ❌ Error al filtrar por tipo de página: {e}")
            return []
    
    def get_alerts_by_status(self, status: str) -> List[Dict]:
        """Filtra alertas por estado"""
        try:
            alerts = list(self.collection.find({'status': status}).sort('date', -1))
            return alerts
        except Exception as e:
            print(f"  ❌ Error al filtrar por estado: {e}")
            return []
    
    def get_alerts_by_date_range(self, start_date: str, end_date: str) -> List[Dict]:
        """
        Filtra alertas por rango de fechas
        
        Args:
            start_date: Fecha inicial (formato ISO)
            end_date: Fecha final (formato ISO)
        """
        try:
            alerts = list(self.collection.find({
                'date': {
                    '$gte': start_date,
                    '$lte': end_date
                }
            }).sort('date', -1))
            return alerts
        except Exception as e:
            print(f"  ❌ Error al filtrar por rango de fechas: {e}")
            return []
    
    def clear_all_alerts(self):
        """Limpia todas las alertas"""
        try:
            result = self.collection.delete_many({})
            print(f"  ✅ {result.deleted_count} alertas eliminadas")
        except Exception as e:
            print(f"  ❌ Error al limpiar alertas: {e}")
    
    def get_stats(self) -> Dict:
        """
        Retorna estadísticas sobre las alertas almacenadas
        
        Returns:
            Diccionario con estadísticas
        """
        try:
            total = self.collection.count_documents({})
            
            # Alertas por país
            countries_agg = list(self.collection.aggregate([
                {'$group': {'_id': '$country', 'count': {'$sum': 1}}}
            ]))
            
            # Alertas por tipo de página
            page_types_agg = list(self.collection.aggregate([
                {'$group': {'_id': '$page_type', 'count': {'$sum': 1}}}
            ]))
            
            # Alertas por estado
            status_agg = list(self.collection.aggregate([
                {'$group': {'_id': '$status', 'count': {'$sum': 1}}}
            ]))
            
            return {
                'total_alerts': total,
                'by_country': countries_agg,
                'by_page_type': page_types_agg,
                'by_status': status_agg
            }
        except Exception as e:
            print(f"  ❌ Error al obtener estadísticas: {e}")
            return {
                'total_alerts': 0,
                'by_country': [],
                'by_page_type': [],
                'by_status': []
            }
    
    def close(self):
        """Cierra la conexión a MongoDB"""
        if self.client:
            self.client.close()
            print("  🔒 Conexión a MongoDB cerrada")


if __name__ == "__main__":
    # Test básico
    storage = AlertStorage()
    
    # Test: agregar alertas
    test_alerts = [
        {
            'date': datetime.now().isoformat(),
            'country': 'CL',
            'page_type': 'PDP',
            'component': 'Buy Together',
            'status': 'MISSING_COMPONENT',
            'message': 'Test alert'
        }
    ]
    
    storage.add_alerts(test_alerts)
    print(f"Stats: {storage.get_stats()}")
    
    storage.close()
    print("Test completado")
