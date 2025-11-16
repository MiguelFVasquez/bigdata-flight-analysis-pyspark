from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

class PySparkMetrics:
    def __init__(self, df):
        self.df = df
        self.metrics_results = {}
        self.execution_times = {}
    
    def calculate_all_metrics(self):
        """Calcula todas las métricas con PySpark - Optimizado"""
        metrics = [
            self.top_airports,
            self.top_airlines, 
            self.aircraft_models_analysis,
            self.popular_routes,
            self.flight_duration_analysis,
            self.airline_fleet_diversity,
            self.airport_connectivity
        ]
        
        print("  Calculando métricas con PySpark...")
        
        for metric_func in metrics:
            func_name = metric_func.__name__
            start_time = time.time()
            try:
                result = metric_func()
                
                # Forzar ejecución y capturar resultados
                processed_result = self._process_spark_result(result, func_name)
                self.metrics_results[func_name] = processed_result
                self.execution_times[func_name] = time.time() - start_time
                print(f"    ✓ {func_name}: {self.execution_times[func_name]:.4f}s")
                
            except Exception as e:
                print(f"    ✗ {func_name} falló: {e}")
                self.execution_times[func_name] = None
        
        return self.metrics_results, self.execution_times
    
    def _process_spark_result(self, result, func_name):
        """Procesa resultados de Spark para hacerlos serializables"""
        if isinstance(result, dict):
            processed_dict = {}
            for key, value in result.items():
                if hasattr(value, 'collect'):
                    df_result = value.collect()
                    processed_dict[key] = [row.asDict() for row in df_result]
                elif hasattr(value, 'count'):
                    value.count()  # Solo forzar ejecución
                    processed_dict[key] = f"DataFrame[{value.count()} rows]"
                else:
                    processed_dict[key] = value
            return processed_dict
        elif hasattr(result, 'collect'):
            df_result = result.collect()
            return [row.asDict() for row in df_result]
        elif hasattr(result, 'count'):
            result.count()  # Forzar ejecución
            return f"DataFrame[{result.count()} rows]"
        else:
            return result
    
    def top_airports(self):
        """Aeropuertos con más operaciones - Distribuido"""
        if 'estdepartureairport' not in self.df.columns:
            return {'error': 'Columna estdepartureairport no encontrada'}
            
        departures = self.df.groupBy("estdepartureairport") \
            .count() \
            .orderBy(F.desc("count")) \
            .limit(10)
        
        arrivals = self.df.groupBy("estarrivalairport") \
            .count() \
            .orderBy(F.desc("count")) \
            .limit(10)
        
        return {
            'departures': departures,
            'arrivals': arrivals
        }
    
    def top_airlines(self):
        """Aerolíneas más activas"""
        if 'callsign' not in self.df.columns:
            return {'error': 'Columna callsign no encontrada'}
            
        return self.df.groupBy("callsign") \
            .count() \
            .orderBy(F.desc("count")) \
            .limit(15)
    
    def aircraft_models_analysis(self):
        """Análisis de modelos de aeronaves"""
        if 'model' not in self.df.columns:
            return {'error': 'Columna model no encontrada'}
            
        top_models = self.df.groupBy("model") \
            .count() \
            .orderBy(F.desc("count")) \
            .limit(10)
        
        result = {'top_models': top_models}
        
        # Duración promedio si existe
        if 'flight_duration' in self.df.columns:
            duration_stats = self.df.groupBy("model") \
                .agg(
                    F.mean("flight_duration").alias("avg_duration"),
                    F.count("*").alias("total_flights"),
                    F.stddev("flight_duration").alias("std_duration")
                ) \
                .orderBy(F.desc("total_flights")) \
                .limit(10)
            result['duration_stats'] = duration_stats
        
        return result
    
    def popular_routes(self):
        """Rutas más frecuentadas"""
        if 'estdepartureairport' not in self.df.columns or 'estarrivalairport' not in self.df.columns:
            return {'error': 'Columnas de aeropuertos no encontradas'}
            
        return self.df.groupBy("estdepartureairport", "estarrivalairport") \
            .count() \
            .orderBy(F.desc("count")) \
            .limit(10)
    
    
    
    def airline_fleet_diversity(self):
        """Diversidad de flota por aerolínea"""
        if 'callsign' not in self.df.columns or 'model' not in self.df.columns:
            return {'error': 'Columnas callsign o model no encontradas'}
            
        return self.df.groupBy("callsign") \
            .agg(F.approx_count_distinct("model").alias("unique_models")) \
            .orderBy(F.desc("unique_models")) \
            .limit(10)
    
    def airport_connectivity(self):
        """Conectividad de aeropuertos"""
        if 'estdepartureairport' not in self.df.columns or 'estarrivalairport' not in self.df.columns:
            return {'error': 'Columnas de aeropuertos no encontradas'}
            
        return self.df.groupBy("estdepartureairport") \
            .agg(F.approx_count_distinct("estarrivalairport").alias("unique_destinations")) \
            .orderBy(F.desc("unique_destinations")) \
            .limit(10)
    
    def flight_duration_analysis(self):
        """Análisis de duración de vuelos"""
        if 'flight_duration' not in self.df.columns:
            return {'error': 'Columna flight_duration no encontrada'}
        
        try:
            # Calcular estadísticas básicas
            stats_df = self.df.select(
                F.mean("flight_duration").alias("mean"),
                F.stddev("flight_duration").alias("stddev"),
                F.min("flight_duration").alias("min"),
                F.max("flight_duration").alias("max"),
                F.count("flight_duration").alias("count")
            )
            
            # Forzar ejecución y obtener valores
            stats_row = stats_df.collect()[0]
            stats_dict = {
                'mean': stats_row['mean'],
                'stddev': stats_row['stddev'],
                'min': stats_row['min'],
                'max': stats_row['max'],
                'count': stats_row['count']
            }
            
            # Calcular percentiles - manejar el caso de lista vacía
            percentiles_values = self.df.approxQuantile("flight_duration", [0.25, 0.5, 0.75, 0.9, 0.95], 0.01)
            
            percentiles_dict = {}
            if percentiles_values and len(percentiles_values) == 5:
                percentile_names = ['25%', '50%', '75%', '90%', '95%']
                percentiles_dict = dict(zip(percentile_names, percentiles_values))
            else:
                percentiles_dict = {'error': 'No se pudieron calcular percentiles'}
            
            return {
                'basic_stats': stats_dict,
                'percentiles': percentiles_dict
            }
            
        except Exception as e:
            return {'error': f'Error en flight_duration_analysis: {str(e)}'}