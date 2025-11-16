import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import *

class DataCleaner:
    def __init__(self):
        self.airport_columns = ['estdepartureairport', 'estarrivalairport']
    
    def clean_pandas(self, df):
        """Limpieza exhaustiva con Pandas"""
        df_clean = df.copy()
        
        print(f"  Columnas disponibles en Pandas: {list(df_clean.columns)}")
        
        # Manejo de valores nulos
        for col in self.airport_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].fillna('UNKNOWN')
        
        if 'callsign' in df_clean.columns:
            df_clean['callsign'] = df_clean['callsign'].fillna('UNKNOWN')
        
        if 'model' in df_clean.columns:
            df_clean['model'] = df_clean['model'].fillna('UNKNOWN')
        
        # Calcular duración de vuelo si tenemos las columnas de tiempo
        if 'firstseen' in df_clean.columns and 'lastseen' in df_clean.columns:
            df_clean['flight_duration'] = df_clean['lastseen'] - df_clean['firstseen']
            
            # Filtrar datos inconsistentes
            mask = (df_clean['flight_duration'] > 0) & (df_clean['flight_duration'] < 24 * 3600)
            df_clean = df_clean[mask]
        
        # Limpiar textos
        text_columns = ['callsign', 'model', 'estdepartureairport', 'estarrivalairport']
        for col in text_columns:
            if col in df_clean.columns and df_clean[col].dtype == 'object':
                df_clean[col] = df_clean[col].str.strip()
        
        print(f"  Datos limpiados con Pandas: {len(df_clean)} filas")
        return df_clean
    
    def clean_pyspark(self, df):
        """Limpieza distribuida con PySpark"""
        from pyspark.sql import functions as F
        
        print(f"  Columnas disponibles en PySpark: {df.columns}")
        
        # Manejo de valores nulos
        fill_values = {}
        for col in self.airport_columns:
            if col in df.columns:
                fill_values[col] = 'UNKNOWN'
        
        if 'callsign' in df.columns:
            fill_values['callsign'] = 'UNKNOWN'
        
        if 'model' in df.columns:
            fill_values['model'] = 'UNKNOWN'
        
        df_clean = df.fillna(fill_values)
        
        # Calcular duración de vuelo y filtrar
        if 'firstseen' in df.columns and 'lastseen' in df.columns:
            df_clean = df_clean.withColumn(
                "flight_duration", 
                F.col("lastseen") - F.col("firstseen")
            )
            
            df_clean = df_clean.filter(
                (F.col("flight_duration") > 0) & 
                (F.col("flight_duration") < 24 * 3600)
            )
        
        # Limpiar textos
        text_columns = ['callsign', 'model', 'estdepartureairport', 'estarrivalairport']
        for column in text_columns:
            if column in df_clean.columns:
                df_clean = df_clean.withColumn(column, F.trim(F.col(column)))
        
        count = df_clean.count()
        print(f"  Datos limpiados con PySpark: {count} filas")
        return df_clean