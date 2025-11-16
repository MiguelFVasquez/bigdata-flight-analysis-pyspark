import pandas as pd
import os

def debug_dataset(file_path):
    """Función para debug del dataset"""
    print(f"\n=== DEBUG DATASET: {file_path} ===")
    
    # Verificar que el archivo existe
    if not os.path.exists(file_path):
        print(f"✗ El archivo no existe: {file_path}")
        return
    
    print(f"✓ Archivo existe. Tamaño: {os.path.getsize(file_path) / (1024*1024):.2f} MB")
    
    # Leer primeras líneas para ver estructura
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = []
            for i in range(5):
                line = f.readline().strip()
                if line:
                    lines.append(line)
            
        print("\nPrimeras 5 líneas del archivo:")
        for i, line in enumerate(lines):
            print(f"{i+1}: {line}")
            
        # Intentar leer con pandas
        print("\nIntentando leer con pandas...")
        df_sample = pd.read_csv(file_path, nrows=10)
        print(f"✓ Pandas puede leer el archivo")
        print(f"Columnas: {list(df_sample.columns)}")
        print(f"Forma: {df_sample.shape}")
        print("\nPrimeras filas:")
        print(df_sample.head(3))
        
    except Exception as e:
        print(f"✗ Error leyendo archivo: {e}")
        # Intentar con diferentes encodings
        encodings = ['latin-1', 'iso-8859-1', 'cp1252']
        for encoding in encodings:
            try:
                df_sample = pd.read_csv(file_path, nrows=5, encoding=encoding)
                print(f"✓ Funciona con encoding: {encoding}")
                print(f"Columnas: {list(df_sample.columns)}")
                break
            except:
                continue

if __name__ == "__main__":
    debug_dataset("data/raw/flight_sample_2022-09-13.csv")