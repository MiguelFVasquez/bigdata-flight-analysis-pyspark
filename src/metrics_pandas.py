import pandas as pd
import time

class PandasMetrics:
    def __init__(self, df):
        self.df = df
        self.metrics_results = {}
        self.execution_times = {}
    
    def calculate_all_metrics(self):
        """Calcula todas las métricas con Pandas"""
        metrics = [
            self.top_airports,
            self.top_airlines,
            self.aircraft_models_analysis,
            self.popular_routes,
            self.flight_duration_analysis,
            self.airline_fleet_diversity,
            self.airport_connectivity
        ]
        
        print("  Calculando métricas con Pandas...")
        
        for metric_func in metrics:
            func_name = metric_func.__name__
            start_time = time.time()
            try:
                result = metric_func()
                self.metrics_results[func_name] = result
                self.execution_times[func_name] = time.time() - start_time
                print(f"    ✓ {func_name}: {self.execution_times[func_name]:.4f}s")
            except Exception as e:
                print(f"    ✗ {func_name} falló: {e}")
                self.execution_times[func_name] = None
        
        return self.metrics_results, self.execution_times
    
    def top_airports(self):
        """Aeropuertos con más operaciones"""
        if 'estdepartureairport' not in self.df.columns:
            return {'error': 'Columna estdepartureairport no encontrada'}
            
        # Conteo de salidas
        departures = self.df['estdepartureairport'].value_counts().head(10)
        # Conteo de llegadas  
        arrivals = self.df['estarrivalairport'].value_counts().head(10)
        
        return {
            'top_departure_airports': departures.to_dict(),
            'top_arrival_airports': arrivals.to_dict()
        }
    
    def top_airlines(self):
        """Aerolíneas más activas"""
        if 'callsign' not in self.df.columns:
            return {'error': 'Columna callsign no encontrada'}
            
        airlines = self.df['callsign'].value_counts().head(15)
        return airlines.to_dict()
    
    def aircraft_models_analysis(self):
        """Análisis de modelos de aeronaves"""
        if 'model' not in self.df.columns:
            return {'error': 'Columna model no encontrada'}
            
        model_stats = self.df['model'].value_counts().head(10)
        
        result = {
            'top_models': model_stats.to_dict()
        }
        
        # Duración promedio por modelo si existe
        if 'flight_duration' in self.df.columns:
            duration_by_model = self.df.groupby('model')['flight_duration'].agg([
                'mean', 'count', 'std'
            ]).round(2).sort_values('count', ascending=False).head(10)
            result['duration_by_model'] = duration_by_model.to_dict('index')
        
        return result
    
    def popular_routes(self):
        """Rutas más frecuentadas"""
        if 'estdepartureairport' not in self.df.columns or 'estarrivalairport' not in self.df.columns:
            return {'error': 'Columnas de aeropuertos no encontradas'}
            
        routes = self.df.groupby(['estdepartureairport', 'estarrivalairport']).size()
        return routes.sort_values(ascending=False).head(10).to_dict()
    
    def flight_duration_analysis(self):
        """Análisis de duración de vuelos"""
        if 'flight_duration' not in self.df.columns:
            return {'error': 'Columna flight_duration no encontrada'}
            
        stats = self.df['flight_duration'].describe()
        
        # Por percentiles
        percentiles = self.df['flight_duration'].quantile([0.25, 0.5, 0.75, 0.9, 0.95])
        
        return {
            'basic_stats': stats.to_dict(),
            'percentiles': percentiles.to_dict()
        }
    
    def airline_fleet_diversity(self):
        """Diversidad de flota por aerolínea"""
        if 'callsign' not in self.df.columns or 'model' not in self.df.columns:
            return {'error': 'Columnas callsign o model no encontradas'}
            
        airline_fleet = self.df.groupby('callsign')['model'].nunique()
        return airline_fleet.sort_values(ascending=False).head(10).to_dict()
    
    def airport_connectivity(self):
        """Conectividad de aeropuertos (destinos únicos)"""
        if 'estdepartureairport' not in self.df.columns or 'estarrivalairport' not in self.df.columns:
            return {'error': 'Columnas de aeropuertos no encontradas'}
            
        airport_connections = self.df.groupby('estdepartureairport')['estarrivalairport'].nunique()
        return airport_connections.sort_values(ascending=False).head(10).to_dict()