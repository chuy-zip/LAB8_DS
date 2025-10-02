# archivo para poder juntar todos los archivos de Fallecidos y Lesionados, Hechos de tránsito Vehículos involucrados

import pandas as pd
import pyreadstat
import os
from pathlib import Path
import re

def detectar_formato_archivos(directorio):
    """Detecta los formatos de archivo disponibles por tipo"""
    tipos = {
        'hechos_transito': [],
        'vehiculos_involucrados': [],
        'fallecidos_lesionados': []
    }
    
    for archivo in Path(directorio).iterdir():
        if archivo.suffix in ['.sav', '.xlsx']:
            nombre = archivo.stem.lower()
            if 'hechos_transito' in nombre:
                tipos['hechos_transito'].append(archivo)
            elif 'vehiculos_involucrados' in nombre:
                tipos['vehiculos_involucrados'].append(archivo)
            elif 'fallecidos_lesionados' in nombre:
                tipos['fallecidos_lesionados'].append(archivo)
    
    return tipos

def extraer_anio(nombre_archivo):
    """Extrae el año del nombre del archivo"""
    match = re.search(r'(\d{4})', nombre_archivo.stem)
    return match.group(1) if match else "Desconocido"

def leer_archivo(archivo):
    """Lee un archivo según su formato (SPSS o Excel)"""
    try:
        if archivo.suffix == '.sav':
            df, meta = pyreadstat.read_sav(archivo)
            print(f"   {archivo.name}: {len(df)} registros, {len(df.columns)} columnas (SPSS)")
            return df
        elif archivo.suffix == '.xlsx':
            df = pd.read_excel(archivo)
            print(f"   {archivo.name}: {len(df)} registros, {len(df.columns)} columnas (Excel)")
            return df
    except Exception as e:
        print(f"   Error leyendo {archivo.name}: {e}")
        return None

def unir_archivos_por_tipo(lista_archivos, tipo):
    """Une archivos del mismo tipo manejando discrepancias de columnas"""
    print(f"\n Procesando archivos de {tipo}:")
    
    dataframes = []
    problemas = []
    
    for archivo in sorted(lista_archivos):
        año = extraer_anio(archivo)
        print(f"  Año {año}: {archivo.name}")
        
        df = leer_archivo(archivo)
        if df is not None:
            # Agregar metadatos
            df['año'] = año
            df['archivo_origen'] = archivo.name
            df['tipo_dataset'] = tipo
            
            dataframes.append(df)
        else:
            problemas.append(archivo.name)
    
    if dataframes:
        # Unir todos los DataFrames manejando columnas diferentes
        df_final = pd.concat(dataframes, ignore_index=True, sort=False)
        
        print(f"\n {tipo} - Resultado:")
        print(f"   - Total registros: {len(df_final)}")
        print(f"   - Total columnas: {len(df_final.columns)}")
        print(f"   - Años incluidos: {sorted(df_final['año'].unique())}")
        print(f"   - Columnas: {list(df_final.columns)}")
        
        return df_final
    else:
        print(f" No se pudo procesar ningún archivo de {tipo}")
        return None

def analizar_estructura_columnas(dfs_unidos):
    """Analiza las columnas comunes y diferentes entre datasets"""
    print("\n" + "="*60)
    print(" ANÁLISIS DE ESTRUCTURA DE COLUMNAS")
    print("="*60)
    
    for tipo, df in dfs_unidos.items():
        if df is not None:
            print(f"\n{tipo.upper()}:")
            print(f"  Columnas ({len(df.columns)}): {list(df.columns)}")
            
            # Verificar valores nulos por columna
            print(f"  Valores nulos por columna:")
            for col in df.columns:
                nulos = df[col].isna().sum()
                if nulos > 0:
                    print(f"    - {col}: {nulos} nulos ({nulos/len(df)*100:.1f}%)")

def guardar_datasets(dfs_unidos, directorio_salida):
    """Guarda los datasets unidos en CSV"""
    Path(directorio_salida).mkdir(exist_ok=True)
    
    for tipo, df in dfs_unidos.items():
        if df is not None:
            archivo_salida = Path(directorio_salida) / f"{tipo}_completo.csv"
            df.to_csv(archivo_salida, index=False, encoding='utf-8-sig')
            print(f" {tipo} guardado en: {archivo_salida}")

def main():
    # Configuración
    directorio_datos = "./data"
    directorio_salida = "./datasets_unidos"
    
    print("Archivoss...")
    tipos_archivos = detectar_formato_archivos(directorio_datos)
    
    # Mostrar archivos encontrados
    for tipo, archivos in tipos_archivos.items():
        print(f"\n{tipo}:")
        for archivo in sorted(archivos):
            print(f"  - {archivo.name}")
    
    # Unir archivos por tipo
    dfs_unidos = {}
    for tipo, archivos in tipos_archivos.items():
        if archivos:
            dfs_unidos[tipo] = unir_archivos_por_tipo(archivos, tipo)
        else:
            print(f"\n  No se encontraron archivos para {tipo}")
            dfs_unidos[tipo] = None
    
    # Análisis de estructura
    analizar_estructura_columnas(dfs_unidos)
    
    # Guardar resultados
    print(f"\n Guardando datasets")
    guardar_datasets(dfs_unidos, directorio_salida)
    
    # Resumen final
    print(f"\n PROCESO COMPLETADO")
    print("="*40)
    for tipo, df in dfs_unidos.items():
        if df is not None:
            print(f"{tipo}: {len(df):,} registros, {len(df.columns)} columnas")
        else:
            print(f"{tipo}: No procesado")

if __name__ == "__main__":
    main()