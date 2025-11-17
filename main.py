import time
import pandas as pd
from src.data_loader import DataLoader
from src.data_cleaner import DataCleaner
from src.metrics_pandas import PandasMetrics
from src.metrics_pyspark import PySparkMetrics
from src.performance_comparison import PerformanceComparator
from src.utils import setup_logging, save_results

def main():
    setup_logging()
    
    # Configuración - Rutas y tamaños de muestra
    DATA_PATH = "data/raw/flight_sample_2022-09-13.csv"
    
    # Empezar con muestras más pequeñas para pruebas
    SAMPLE_SIZES = [1000, 5000, 10000, "full"]  
    results = []
    
    for sample_size in SAMPLE_SIZES:
        print(f"\n{'='*50}")
        print(f"PROCESANDO MUESTRA: {sample_size}")
        print(f"{'='*50}")
        
        try:
            # Carga de datos
            loader = DataLoader(DATA_PATH)
            
            # Python/Pandas
            print("Cargando datos con Pandas...")
            start_time = time.time()
            df_pandas = loader.load_pandas(sample_size)
            pandas_load_time = time.time() - start_time
            print(f"✓ Pandas cargado: {len(df_pandas)} filas en {pandas_load_time:.2f}s")
            
            # PySpark
            print("Cargando datos con PySpark...")
            start_time = time.time()
            df_spark = loader.load_pyspark(sample_size)
            if df_spark:
                spark_count = df_spark.count()
            else:
                spark_count = 0
            spark_load_time = time.time() - start_time
            print(f"✓ PySpark cargado: {spark_count} filas en {spark_load_time:.2f}s")
            
            # Limpieza de datos
            print("Limpiando datos...")
            cleaner = DataCleaner()
            df_pandas_clean = cleaner.clean_pandas(df_pandas)
            print(f"✓ Pandas limpiado: {len(df_pandas_clean)} filas")
            
            if df_spark:
                df_spark_clean = cleaner.clean_pyspark(df_spark)
                spark_clean_count = df_spark_clean.count()
                print(f"✓ PySpark limpiado: {spark_clean_count} filas")
            else:
                df_spark_clean = None
            
            # Métricas con Pandas
            print("Calculando métricas con Pandas...")
            pandas_metrics = PandasMetrics(df_pandas_clean)
            pandas_results, pandas_times = pandas_metrics.calculate_all_metrics()
            print("✓ Métricas Pandas completadas")
            
            # Métricas con PySpark
            spark_results, spark_times = {}, {}
            if df_spark_clean:
                print("Calculando métricas con PySpark...")
                spark_metrics = PySparkMetrics(df_spark_clean)
                spark_results, spark_times = spark_metrics.calculate_all_metrics()
                print("✓ Métricas PySpark completadas")
            else:
                print("✗ Saltando métricas PySpark por error en carga")
            
            # Comparación
            if pandas_times and spark_times:
                comparator = PerformanceComparator(
                    pandas_times, spark_times, pandas_results, spark_results
                )
                comparison_results = comparator.compare_performance()
                comparison_report = comparator.generate_performance_report()
                print("\n" + comparison_report)
                results.append({
                    'sample_size': sample_size,
                    'pandas_load_time': pandas_load_time,
                    'spark_load_time': spark_load_time,
                    'comparison': comparison_results
                })
                
                print(f"✓ Comparación completada para muestra {sample_size}")
            else:
                print("✗ No se pudo realizar comparación")
                
        except Exception as e:
            print(f"✗ Error procesando muestra {sample_size}: {e}")
            import traceback
            traceback.print_exc()
    
    # Guardar resultados
    if results:
        save_results(results, "results/performance_metrics.csv")
        print("✓ Resultados guardados exitosamente")
        
        # Generar visualizaciones si hay datos
        try:
            comparator.generate_visualizations()
            print("✓ Visualizaciones generadas")
        except:
            print("✗ No se pudieron generar visualizaciones")
    else:
        print("✗ No hay resultados para guardar")

if __name__ == "__main__":
    main()