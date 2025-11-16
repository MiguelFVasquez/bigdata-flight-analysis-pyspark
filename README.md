# 🛫 Análisis de Patrones de Vuelo - PySpark vs Pandas

Proyecto final de Big Data que compara el rendimiento de **PySpark** vs **Python/Pandas** en el análisis de datos de tráfico aéreo global.

## Descripción del Proyecto

Este proyecto demuestra las ventajas de PySpark sobre técnicas tradicionales de procesamiento con Python/Pandas mediante el análisis de datasets masivos de vuelos. Utiliza datos reales de **OpenSky Network** para analizar patrones de operaciones aéreas.

### Objetivos Principales

- **Comparar rendimiento** entre PySpark y Pandas
- **Analizar patrones** de vuelo globales
- **Identificar rutas** más congestionadas
- **Calcular métricas** de operaciones aéreas
- **Demostrar escalabilidad** en procesamiento de datos

## 🏗️ Estructura del Proyecto

* src/
    - data_loader.py # Carga de datos (Pandas + PySpark)
    - data_cleaner.py # Limpieza y preprocesamiento
    - metrics_pandas.py # Métricas con Pandas (optimizadas)
    - metrics_pyspark.py # Métricas con PySpark (distribuidas)
    -  performance_comparison.py # Comparación y visualizaciones
    - utils.py # Utilidades y logging
* data/
    - raw/
        - flight_sample_2022-09-13.csv # Dataset original
    - processed/
* results/
    - performance_metrics.csv # Resultados de timing
    - performance_report.txt # Reporte ejecutivo
    - visualizations/
        - performance_comparison.png
        - speedup_comparison.png
        - performance_trend.png
* config/
    - settings.py # Configuración del proyecto
* main.py # Código de ejecución principal
* requirements.txt # Dependencias
* README.md


## 📈 Métricas Implementadas

### 🔍 Métricas de Operaciones Aéreas
- **Top Aeropuertos**: Aeropuertos con más operaciones (salidas/llegadas)
- **Top Aerolíneas**: Compañías más activas por número de vuelos
- **Rutas Populares**: Pares origen-destino más frecuentados
- **Conectividad**: Aeropuertos con mayor diversidad de destinos

### ✈️ Métricas de Flota y Modelos
- **Modelos Comunes**: Tipos de aeronaves más utilizados
- **Diversidad de Flota**: Variedad de modelos por aerolínea
- **Duración de Vuelo**: Análisis estadístico de tiempos de operación

## Instalación y Ejecución

### Prerrequisitos
- bash
- Python 3.8+
- Java 8+ (para PySpark)

### Instalación de dependencias 

- pip install -r requirements.txt

### Ejecución del proyecto

- python main.py