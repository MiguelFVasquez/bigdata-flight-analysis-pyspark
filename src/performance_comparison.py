import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

class PerformanceComparator:
    def __init__(self, pandas_times, spark_times, pandas_results, spark_results):
        self.pandas_times = pandas_times
        self.spark_times = spark_times
        self.pandas_results = pandas_results
        self.spark_results = spark_results
    
    def compare_performance(self):
        """Comparación exhaustiva del rendimiento"""
        comparison = {}
        
        for metric in self.pandas_times.keys():
            if (self.pandas_times[metric] is not None and 
                self.spark_times.get(metric) is not None and
                self.spark_times[metric] is not None):
                
                speedup = self.pandas_times[metric] / self.spark_times[metric]
                comparison[metric] = {
                    'pandas_time': self.pandas_times[metric],
                    'spark_time': self.spark_times[metric],
                    'speedup': speedup,
                    'efficiency_gain': f"{speedup:.2f}x"
                }
        
        return comparison
    
    def generate_visualizations(self):
        """Genera visualizaciones profesionales de la comparación"""
        try:
            # Crear directorio de visualizaciones
            os.makedirs('results/visualizations', exist_ok=True)
            
            # Obtener datos para plotting
            comparison_data = self.compare_performance()
            
            if not comparison_data:
                print("No hay datos de comparación para visualizar")
                return
            
            metrics = list(comparison_data.keys())
            pandas_times = [comparison_data[m]['pandas_time'] for m in metrics]
            spark_times = [comparison_data[m]['spark_time'] for m in metrics]
            
            # Crear DataFrame para plotting
            df_plot = pd.DataFrame({
                'Metric': metrics,
                'Pandas': pandas_times,
                'PySpark': spark_times
            })
            
            # Configurar estilo
            plt.style.use('default')
            sns.set_palette("husl")
            
            # 1. Gráfico de barras comparativo
            self._create_bar_chart(df_plot, metrics, pandas_times, spark_times)
            
            # 2. Gráfico de speedup
            self._create_speedup_chart(comparison_data, metrics)
            
            # 3. Gráfico de líneas de tendencia
            self._create_trend_chart(df_plot)
            
            print("✓ Visualizaciones generadas exitosamente")
            
        except Exception as e:
            print(f"✗ Error generando visualizaciones: {e}")
            import traceback
            traceback.print_exc()
    
    def _create_bar_chart(self, df_plot, metrics, pandas_times, spark_times):
        """Gráfico de barras comparativo"""
        plt.figure(figsize=(14, 8))
        
        # Preparar datos para seaborn
        plot_data = []
        for i, metric in enumerate(metrics):
            plot_data.append({'Metric': metric, 'Time(s)': pandas_times[i], 'Framework': 'Pandas'})
            plot_data.append({'Metric': metric, 'Time(s)': spark_times[i], 'Framework': 'PySpark'})
        
        plot_df = pd.DataFrame(plot_data)
        
        # Crear gráfico
        ax = sns.barplot(data=plot_df, x='Metric', y='Time(s)', hue='Framework')
        plt.title('Comparación de Rendimiento: Pandas vs PySpark', fontsize=16, fontweight='bold')
        plt.xlabel('Métricas', fontsize=12)
        plt.ylabel('Tiempo de Ejecución (segundos)', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        
        # Añadir valores en las barras
        for i, container in enumerate(ax.containers):
            for j, bar in enumerate(container):
                height = bar.get_height()
                if height > 0.01:  # Solo mostrar valores significativos
                    ax.text(bar.get_x() + bar.get_width()/2., height + 0.001,
                           f'{height:.3f}s', ha='center', va='bottom', fontsize=8)
        
        plt.tight_layout()
        plt.savefig('results/visualizations/performance_comparison.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def _create_speedup_chart(self, comparison_data, metrics):
        """Gráfico de speedup"""
        plt.figure(figsize=(12, 6))
        
        speedups = [comparison_data[m]['speedup'] for m in metrics]
        colors = ['lightgreen' if s > 1 else 'lightcoral' for s in speedups]
        
        bars = plt.bar(metrics, speedups, color=colors, alpha=0.7)
        plt.axhline(y=1, color='red', linestyle='--', linewidth=2, label='Límite de igualdad')
        
        # Añadir valores en las barras
        for bar, speedup in zip(bars, speedups):
            plt.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 0.05,
                    f'{speedup:.2f}x', ha='center', va='bottom', fontweight='bold')
        
        plt.title('Speedup: PySpark vs Pandas\n(Valores >1 favorecen a PySpark)', 
                 fontsize=14, fontweight='bold')
        plt.ylabel('Speedup (Pandas Time / PySpark Time)', fontsize=12)
        plt.xlabel('Métricas', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('results/visualizations/speedup_comparison.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def _create_trend_chart(self, df_plot):
        """Gráfico de tendencia de performance"""
        plt.figure(figsize=(10, 6))
        
        # Calcular ratio de performance
        df_plot['Performance_Ratio'] = df_plot['Pandas'] / df_plot['PySpark']
        
        plt.plot(df_plot['Metric'], df_plot['Performance_Ratio'], 
                marker='o', linewidth=2, markersize=8, label='Ratio Pandas/PySpark')
        plt.axhline(y=1, color='red', linestyle='--', linewidth=2, label='Límite neutral')
        
        plt.title('Tendencia de Ratio de Performance', fontsize=14, fontweight='bold')
        plt.ylabel('Ratio de Performance (Pandas/PySpark)', fontsize=12)
        plt.xlabel('Métricas', fontsize=12)
        plt.xticks(rotation=45, ha='right')
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig('results/visualizations/performance_trend.png', dpi=300, bbox_inches='tight')
        plt.close()
    
    def generate_performance_report(self):
        """Genera un reporte de performance detallado"""
        comparison = self.compare_performance()
        
        if not comparison:
            return "No hay datos de comparación disponibles"
        
        report = []
        report.append("=" * 60)
        report.append("REPORTE DE PERFORMANCE: PANDAS vs PYSPARK")
        report.append("=" * 60)
        
        for metric, data in comparison.items():
            report.append(f"\nMétrica: {metric}")
            report.append(f"  Pandas:   {data['pandas_time']:.4f} segundos")
            report.append(f"  PySpark:  {data['spark_time']:.4f} segundos")
            report.append(f"  Speedup:  {data['efficiency_gain']}")
            
            if data['speedup'] > 1:
                report.append(f"  → PySpark es {data['speedup']:.2f}x más rápido")
            else:
                report.append(f"  → Pandas es {1/data['speedup']:.2f}x más rápido")
        
        # Resumen general
        speedups = [data['speedup'] for data in comparison.values()]
        avg_speedup = sum(speedups) / len(speedups)
        
        report.append("\n" + "=" * 60)
        report.append("RESUMEN EJECUTIVO")
        report.append("=" * 60)
        report.append(f"Speedup promedio: {avg_speedup:.2f}x")
        
        if avg_speedup > 1:
            report.append("CONCLUSIÓN: PySpark muestra mejor performance general")
        else:
            report.append("CONCLUSIÓN: Pandas muestra mejor performance general")
        report.append("=" * 60)
        
        report_text = "\n".join(report)
        
        # Guardar reporte
        with open('results/performance_report.txt', 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        return report_text