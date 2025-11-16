import logging
import os
import json
import pandas as pd
from datetime import datetime
import time
from typing import Dict, Any, List

def setup_logging(log_level=logging.INFO, log_file="logs/project.log"):
    """Configura el sistema de logging para el proyecto"""
    
    # Crear directorio de logs si no existe
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Logging configurado correctamente")
    return logger

def save_results(results: Dict[str, Any], file_path: str):
    """Guarda los resultados en formato CSV o JSON"""
    
    # Crear directorio si no existe
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    try:
        if file_path.endswith('.csv'):
            # Para resultados tabulares
            if isinstance(results, list):
                df = pd.DataFrame(results)
                df.to_csv(file_path, index=False)
            else:
                # Convertir diccionario anidado a DataFrame
                flat_results = flatten_dict(results)
                pd.DataFrame([flat_results]).to_csv(file_path, index=False)
                
        elif file_path.endswith('.json'):
            # Para resultados complejos con estructura
            with open(file_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
        
        logging.info(f"Resultados guardados en: {file_path}")
        
    except Exception as e:
        logging.error(f"Error guardando resultados en {file_path}: {e}")

def load_results(file_path: str):
    """Carga resultados desde archivo"""
    try:
        if file_path.endswith('.csv'):
            return pd.read_csv(file_path)
        elif file_path.endswith('.json'):
            with open(file_path, 'r') as f:
                return json.load(f)
    except Exception as e:
        logging.error(f"Error cargando resultados desde {file_path}: {e}")
        return None

def flatten_dict(nested_dict: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """Convierte un diccionario anidado en uno plano"""
    items = []
    for k, v in nested_dict.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def timer(func):
    """Decorator para medir tiempo de ejecución de funciones"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logging.info(f"Función {func.__name__} ejecutada en {execution_time:.4f} segundos")
        return result, execution_time
    return wrapper

def memory_usage():
    """Estima el uso de memoria (aproximado)"""
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        return {
            'rss_mb': memory_info.rss / 1024 / 1024,  # Resident Set Size
            'vms_mb': memory_info.vms / 1024 / 1024   # Virtual Memory Size
        }
    except ImportError:
        logging.warning("psutil no instalado. No se puede medir uso de memoria.")
        return {'rss_mb': None, 'vms_mb': None}

def validate_dataframe(df, framework: str = "pandas"):
    """Valida la calidad básica de un DataFrame"""
    validation_results = {}
    
    try:
        if framework == "pandas":
            validation_results = {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'columns': list(df.columns),
                'null_counts': df.isnull().sum().to_dict(),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
            }
        elif framework == "pyspark":
            validation_results = {
                'total_rows': df.count(),
                'total_columns': len(df.columns),
                'columns': df.columns,
                'null_counts': {col: df.filter(df[col].isNull()).count() for col in df.columns},
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes},
                'partitions': df.rdd.getNumPartitions()
            }
        
        logging.info(f"Validación {framework.upper()}: {validation_results['total_rows']} filas, {validation_results['total_columns']} columnas")
        return validation_results
        
    except Exception as e:
        logging.error(f"Error en validación de DataFrame ({framework}): {e}")
        return {}

def generate_report(performance_results: List[Dict], output_dir: str = "results"):
    """Genera un reporte completo del proyecto"""
    
    os.makedirs(output_dir, exist_ok=True)
    report_data = {
        'timestamp': datetime.now().isoformat(),
        'project': 'Análisis de Patrones de Vuelo - Big Data',
        'performance_comparison': performance_results,
        'summary': generate_summary(performance_results)
    }
    
    # Guardar reporte detallado
    with open(f"{output_dir}/full_report.json", 'w') as f:
        json.dump(report_data, f, indent=2, default=str)
    
    # Generar resumen ejecutivo en CSV
    summary_df = generate_executive_summary(performance_results)
    summary_df.to_csv(f"{output_dir}/executive_summary.csv", index=False)
    
    logging.info("Reporte generado exitosamente")

def generate_summary(performance_results: List[Dict]) -> Dict[str, Any]:
    """Genera un resumen de los resultados de performance"""
    
    if not performance_results:
        return {}
    
    # Usar el último resultado (dataset completo)
    last_result = performance_results[-1]
    comparison = last_result.get('comparison', {})
    
    if not comparison:
        return {}
    
    avg_speedup = sum([metric['speedup'] for metric in comparison.values()]) / len(comparison)
    max_speedup_metric = max(comparison.items(), key=lambda x: x[1]['speedup'])
    min_speedup_metric = min(comparison.items(), key=lambda x: x[1]['speedup'])
    
    return {
        'average_speedup': avg_speedup,
        'max_speedup': {
            'metric': max_speedup_metric[0],
            'speedup': max_speedup_metric[1]['speedup']
        },
        'min_speedup': {
            'metric': min_speedup_metric[0],
            'speedup': min_speedup_metric[1]['speedup']
        },
        'total_metrics_compared': len(comparison),
        'recommendation': generate_recommendation(avg_speedup)
    }

def generate_executive_summary(performance_results: List[Dict]) -> pd.DataFrame:
    """Genera un resumen ejecutivo en formato DataFrame"""
    
    summary_data = []
    for result in performance_results:
        sample_size = result['sample_size']
        comparison = result.get('comparison', {})
        
        for metric, times in comparison.items():
            summary_data.append({
                'sample_size': sample_size,
                'metric': metric,
                'pandas_time_seconds': times['pandas_time'],
                'pyspark_time_seconds': times['spark_time'],
                'speedup': times['speedup'],
                'efficiency_gain': times['efficiency_gain']
            })
    
    return pd.DataFrame(summary_data)

def generate_recommendation(avg_speedup: float) -> str:
    """Genera una recomendación basada en los resultados"""
    
    if avg_speedup > 10:
        return "PySpark es significativamente más eficiente. Recomendado para datasets grandes y procesamiento distribuido."
    elif avg_speedup > 3:
        return "PySpark muestra ventajas claras en performance. Considerar para operaciones complejas y datasets en crecimiento."
    elif avg_speedup > 1:
        return "PySpark tiene ventaja moderada. Evaluar según complejidad de operaciones y tamaño de datos."
    else:
        return "Pandas puede ser suficiente para este volumen de datos. PySpark recomendado solo si se espera crecimiento significativo."

def cleanup_spark_session(spark_session):
    """Limpia la sesión de Spark"""
    try:
        spark_session.stop()
        logging.info("Sesión de Spark detenida correctamente")
    except Exception as e:
        logging.error(f"Error deteniendo sesión de Spark: {e}")

def check_directory_structure():
    """Verifica y crea la estructura de directorios requerida"""
    directories = [
        'data/raw',
        'data/processed', 
        'results/visualizations',
        'logs',
        'src'
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logging.debug(f"Directorio verificado/creado: {directory}")

def get_dataset_info(file_path: str) -> Dict[str, Any]:
    """Obtiene información básica del dataset"""
    try:
        import gzip
        import subprocess
        
        info = {}
        
        # Tamaño del archivo
        info['file_size_mb'] = os.path.getsize(file_path) / (1024 * 1024)
        
        # Número de líneas (aproximado para archivos comprimidos)
        try:
            result = subprocess.run(
                ['zcat', file_path, '|', 'wc', '-l'], 
                shell=True, capture_output=True, text=True
            )
            info['approx_lines'] = int(result.stdout.strip()) if result.stdout else 'Unknown'
        except:
            info['approx_lines'] = 'Unknown'
        
        # Primeras líneas para inspección
        with gzip.open(file_path, 'rt') as f:
            head_lines = [next(f).strip() for _ in range(3)]
            info['sample_data'] = head_lines
        
        logging.info(f"Dataset info: {info['file_size_mb']:.2f} MB")
        return info
        
    except Exception as e:
        logging.error(f"Error obteniendo información del dataset: {e}")
        return {}