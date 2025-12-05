import pandas as pd
import logging

# --- AJUSTA LA RUTA SI ES NECESARIO ---
PATH_BASE = r"U:"
# CAMBIO 1: Actualizamos la extensión a .xlsb
FILE_MASTER_LIST = fr"{PATH_BASE}\COMPROBACION ERRORES BUENO.xlsb"
SHEET_MASTER_LIST = "INGRESO DE ORDENES"

logging.basicConfig(level=logging.INFO)

def debug_headers():
    logging.info(f"--- LEYENDO: {FILE_MASTER_LIST} ---")
    try:
        # Leemos SOLO la fila de encabezados (Fila 3 de Excel = índice 2 en Pandas)
        # nrows=0 lee solo las columnas sin datos
        
        # CAMBIO 2: Usamos engine='pyxlsb' para archivos binarios
        df = pd.read_excel(
            FILE_MASTER_LIST,
            sheet_name=SHEET_MASTER_LIST,
            engine='pyxlsb',   # IMPORTANTE: openpyxl no lee .xlsb
            header=2,          # Fila 3 de Excel
            nrows=0            # No traer datos, solo columnas
        )
        
        columnas = list(df.columns)
        
        # Guardamos en un archivo de texto para que lo puedas leer con calma
        with open("columnas_detectadas.txt", "w", encoding="utf-8") as f:
            f.write(f"ARCHIVO: {FILE_MASTER_LIST}\n")
            f.write(f"Total columnas detectadas: {len(columnas)}\n")
            f.write("-" * 40 + "\n")
            for i, col in enumerate(columnas):
                f.write(f"[{i}] '{col}'\n")
                
        logging.info(f"¡ÉXITO! Se detectaron {len(columnas)} columnas.")
        logging.info(f"Abre el archivo 'columnas_detectadas.txt' para ver los nombres exactos.")
        
    except Exception as e:
        logging.error(f"Error leyendo el archivo: {e}")

if __name__ == "__main__":
    debug_headers()