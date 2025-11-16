# Configuración del proyecto

# Rutas de datos
DATA_PATHS = {
    'raw_flights': 'data/raw/flight_sample_2022-09-13.csv.gz',
    'processed': 'data/processed/'
}

# Configuración de Spark
SPARK_CONFIG = {
    'app_name': 'FlightAnalysisProject',
    'master': 'local[*]',  # Usar todos los cores disponibles
    'configurations': {
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.sql.adaptive.skew.enabled': 'true',
        'spark.sql.autoBroadcastJoinThreshold': '10485760',  # 10MB
        'spark.driver.memory': '2g',
        'spark.executor.memory': '2g'
    }
}

# Parámetros de análisis
ANALYSIS_CONFIG = {
    'sample_sizes': [1000, 10000, 50000, 100000, 'full'],
    'top_n_values': 10,
    'flight_duration_limits': {
        'min_seconds': 60,      # 1 minuto mínimo
        'max_seconds': 86400    # 24 horas máximo
    }
}

# Columnas para análisis
COLUMNS = {
    'key_columns': ['icao24', 'callsign', 'registration'],
    'airport_columns': ['estdepartureairport', 'estarrivalairport'],
    'time_columns': ['firstseen', 'lastseen', 'takeofftime', 'landingtime'],
    'aircraft_columns': ['model', 'typecode']
}