import pandas as pd
from pyspark.sql import SparkSession
import os

class DataLoader:
    def __init__(self, data_path):
        self.data_path = data_path
        self.spark = SparkSession.builder \
            .appName("FlightAnalysis") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("ERROR")
    
    def load_pandas(self, sample_size):
        """Carga datos con Pandas - para comparación"""
        try:
            if sample_size == "full":
                df = pd.read_csv(self.data_path)
            else:
                df = pd.read_csv(self.data_path, nrows=sample_size)
            
            # Limpiar nombres de columnas (eliminar espacios)
            df.columns = [col.strip() for col in df.columns]
            return df
            
        except Exception as e:
            print(f"Error cargando con Pandas: {e}")
            return self._create_sample_data(sample_size)
    
    def load_pyspark(self, sample_size):
        """Carga datos con PySpark - enfoque distribuido"""
        try:
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(self.data_path)
            
            # Renombrar columnas para eliminar espacios
            for col in df.columns:
                df = df.withColumnRenamed(col, col.strip())
            
            if sample_size != "full":
                df = df.limit(sample_size)
            
            return df
        except Exception as e:
            print(f"Error cargando con PySpark: {e}")
            return None
    
    def _create_sample_data(self, sample_size):
        """Crear datos de ejemplo si hay problemas con el archivo"""
        import numpy as np
        airports = ['KJFK', 'KLAX', 'KORD', 'KDEN', 'KSFO', 'KMIA', 'KSEA', 'KBOS']
        models = ['B737', 'A320', 'B787', 'A380', 'CRJ900', 'E175']
        callsigns = ['UAL', 'DAL', 'AAL', 'SWA', 'FDX', 'UPS']
        
        data = {
            'icao24': [f'abc{i:03d}' for i in range(sample_size)],
            'firstseen': np.random.uniform(1663000000, 1663200000, sample_size),
            'lastseen': np.random.uniform(1663100000, 1663300000, sample_size),
            'callsign': np.random.choice(callsigns, sample_size),
            'estdepartureairport': np.random.choice(airports, sample_size),
            'estarrivalairport': np.random.choice(airports, sample_size),
            'model': np.random.choice(models, sample_size)
        }
        return pd.DataFrame(data)