import pandas as pd
import numpy as np
import logging
from datetime import datetime
from pymongo import MongoClient, UpdateOne

# --- CONFIGURACIÓN ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "produccion_db"

# Ajusta la ruta base según tu entorno
PATH_BASE = r"U:"

FILE_MASTER_LIST = fr"{PATH_BASE}\COMPROBACION ERRORES BUENO.xlsb"
FILE_PLANCOR = fr"{PATH_BASE}\PEDIDOS.xlsx"
FILE_TERMINADO = fr"{PATH_BASE}\PRODUCTO TERMINADO.xlsx"

SHEET_MASTER_LIST = "INGRESO DE ORDENES"
SHEET_PLANCOR = 0
SHEET_TERMINADO = 0

# --- RANGO DE LECTURA ---
FILA_CABECERA_EXCEL = 3 
FILA_INICIO_DATOS = 174000 
FILA_FIN_DATOS = 190000

# --- MAPEO DE COLUMNAS ---
# Eliminé TRANSPORTE como pediste.
COLUMNS_MAP = {
    "OP": "OP",
    "Cliente": "CLIENTE",
    "Tipo": "TIPO",
    "Res": "MATERIAL",
    "Flauta": "FLAUTA",
    "Ancho": "ANCHO",
    "Largo": "LARGO",
    "O/C": "OC",
    "Ingreso": "FECHA_INGRESO",   # Columna 17
    "CantPedida": "PIEZAS",
    "DIRECCIÓN DE ENTREGA": "DIRECCION_ENTREGA",
    "Entrega": "FECHA_ENTREGA",   # Columna 23
    "M² INGRESADOS ": "M2"
}

CLIENTES_EXCLUIDOS = ['PYCAPSA TRIM', 'BOX NOW', 'MUESTRAS']

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- FUNCIONES DE LIMPIEZA ---

def limpiar_op(valor):
    """ Convierte cualquier valor de OP a un string de número entero limpio. """
    if pd.isna(valor) or str(valor).strip() == '':
        return None
    try:
        val_float = float(valor)
        val_int = int(val_float)
        return str(val_int)
    except ValueError:
        return str(valor).strip()

def convertir_fecha_excel(valor):
    """
    Convierte el número serial de Excel (ej. 45200) a Datetime real.
    Maneja también si por error viene como texto.
    """
    if pd.isna(valor) or str(valor).strip() == '':
        return None
    
    try:
        # Intento 1: Es un número serial de Excel (float o int)
        # La fecha base de Excel es 1899-12-30
        serial = float(valor)
        return pd.to_datetime(serial, unit='D', origin='1899-12-30')
    except (ValueError, TypeError):
        try:
            # Intento 2: Es un string de fecha (ej '2025-10-01')
            return pd.to_datetime(valor, errors='coerce')
        except:
            return None

def extract_master() -> pd.DataFrame | None:
    logging.info(f"Extrayendo MAESTRA desde: {FILE_MASTER_LIST}")
    try:
        # Paso 1: Leer solo encabezados
        df_header = pd.read_excel(
            FILE_MASTER_LIST,
            sheet_name=SHEET_MASTER_LIST,
            engine='pyxlsb', 
            header=FILA_CABECERA_EXCEL - 1, 
            nrows=0 
        )
        columnas_reales = list(df_header.columns)

        logging.info(f"Leyendo datos rango: {FILA_INICIO_DATOS} a {FILA_FIN_DATOS}...")
        
        idx_inicio = FILA_INICIO_DATOS - 1
        idx_fin = FILA_FIN_DATOS - 1

        def logica_filas_datos(x):
            if x < idx_inicio: return True
            if x > idx_fin: return True
            return False

        # Paso 2: Leer los datos
        df = pd.read_excel(
            FILE_MASTER_LIST,
            sheet_name=SHEET_MASTER_LIST,
            engine='pyxlsb', 
            header=None, 
            skiprows=logica_filas_datos 
        )
        
        # Asignar columnas
        if len(df.columns) == len(columnas_reales):
            df.columns = columnas_reales
        else:
            min_cols = min(len(df.columns), len(columnas_reales))
            df = df.iloc[:, :min_cols]
            df.columns = columnas_reales[:min_cols]

        logging.info(f"Filas leídas correctamente: {len(df)}")
        return df
    except Exception as e:
        logging.error(f"Error al leer la MAESTRA: {e}")
        return None

def extract_plancor() -> pd.DataFrame | None:
    logging.info(f"Extrayendo PLANCOR desde: {FILE_PLANCOR}")
    try:
        df = pd.read_excel(FILE_PLANCOR, sheet_name=SHEET_PLANCOR, usecols="A,AY", engine='openpyxl')
        df.columns = ['op_plancor', 'cantidad_plancor']
        df['op_plancor'] = df['op_plancor'].apply(limpiar_op)
        return df
    except Exception as e:
        logging.error(f"Error al leer PLANCOR: {e}")
        return None

def extract_terminado() -> pd.DataFrame | None:
    logging.info(f"Extrayendo TERMINADO desde: {FILE_TERMINADO}")
    try:
        df = pd.read_excel(FILE_TERMINADO, sheet_name=SHEET_TERMINADO, usecols="A", engine='openpyxl')
        df.columns = ['op_terminado']
        df['op_terminado'] = df['op_terminado'].apply(limpiar_op)
        df.dropna(subset=['op_terminado'], inplace=True)
        df['existe_en_terminado'] = True
        df.drop_duplicates(subset=['op_terminado'], inplace=True)
        return df
    except Exception as e:
        logging.error(f"Error al leer TERMINADO: {e}")
        return None


def transform(df_master, df_plancor, df_terminado) -> pd.DataFrame | None:
    logging.info("Iniciando transformación...")
    
    # Verificación de columnas
    req_cols = list(COLUMNS_MAP.keys())
    faltantes = [c for c in req_cols if c not in df_master.columns]
    
    if faltantes:
        logging.error(f"Faltan columnas en el Excel: {faltantes}")
        logging.info(f"Columnas encontradas: {list(df_master.columns)}")
        return None

    # Seleccionar y Renombrar
    df_master = df_master[req_cols].copy()
    df_master.rename(columns=COLUMNS_MAP, inplace=True)
    
    # Limpieza OP Principal
    df_master['OP'] = df_master['OP'].apply(limpiar_op)
    df_master.dropna(subset=['OP'], inplace=True)
    
    # Conversión de Tipos numéricos
    df_master['M2'] = pd.to_numeric(df_master['M2'], errors='coerce').fillna(0.0)
    for col in ['ANCHO', 'LARGO', 'PIEZAS']:
        df_master[col] = pd.to_numeric(df_master[col], errors='coerce').fillna(0).astype(int)
    
    # --- CORRECCIÓN DE FECHAS AQUÍ ---
    # Usamos la nueva función helper para convertir seriales de Excel
    logging.info("Convirtiendo fechas seriales de Excel...")
    df_master['FECHA_INGRESO'] = df_master['FECHA_INGRESO'].apply(convertir_fecha_excel)
    df_master['FECHA_ENTREGA'] = df_master['FECHA_ENTREGA'].apply(convertir_fecha_excel)
    
    # Filtrado lógico
    df_master.dropna(subset=['FECHA_INGRESO', 'FECHA_ENTREGA'], inplace=True)
    df_master.dropna(subset=['CLIENTE'], inplace=True)
    df_master = df_master[~df_master['CLIENTE'].isin(CLIENTES_EXCLUIDOS)].copy()

    # Rellenar vacíos (Ya NO rellenamos transporte porque se eliminó)
    df_master['CLIENTE'] = df_master['CLIENTE'].fillna('SIN CLIENTE')
    df_master['OC'] = df_master['OC'].fillna('SIN O/C') 
    df_master['TIPO'] = df_master['TIPO'].fillna('SIN TIPO')
    df_master['MATERIAL'] = df_master['MATERIAL'].fillna('SIN DATO')
    df_master['FLAUTA'] = df_master['FLAUTA'].fillna('SIN DATO')
    df_master['DIRECCION_ENTREGA'] = df_master['DIRECCION_ENTREGA'].fillna('SIN DATOS')

    logging.info("Combinando con Plancor y Terminado...")
    df = pd.merge(df_master, df_plancor, left_on='OP', right_on='op_plancor', how='left')
    df = pd.merge(df, df_terminado, left_on='OP', right_on='op_terminado', how='left')

    logging.info("Calculando estatus...")
    conditions = [
        (df['cantidad_plancor'].isna()),
        (df['cantidad_plancor'] == 0),
        (df['cantidad_plancor'] < (df['PIEZAS'] * 0.9)),
        (df['cantidad_plancor'] >= (df['PIEZAS'] * 0.9))
    ]
    choices = ["SIN PROGRAMAR", "INGRESADO SIN PROGRAMAR", "PROGRAMADO PARCIAL", "PROGRAMADO"]
    df['estatus_plancor'] = np.select(conditions, choices, default='ERROR')
    
    df['ESTATUS_EXCEL'] = np.where(
        (df['estatus_plancor'] == 'SIN PROGRAMAR') & (df['existe_en_terminado'].isna()),
        "SIN FABRICAR",
        df['estatus_plancor']
    )
    
    df = df[df['ESTATUS_EXCEL'] != 'PROGRAMADO'].copy()
    
    # Columnas finales a guardar
    cols_finales = list(COLUMNS_MAP.values()) + ['ESTATUS_EXCEL']
    df_final = df[cols_finales].copy()
    df_final = df_final.where(pd.notnull(df_final), None)
    
    logging.info(f"Transformación lista. {len(df_final)} pedidos listos.")
    return df_final

def load(df: pd.DataFrame):
    logging.info("Cargando a MongoDB...")
    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db["pedidos"]

        operations = []
        for _, row in df.iterrows():
            doc = row.to_dict()
            op_id = limpiar_op(doc.pop("OP"))
            if op_id:
                operations.append(
                    UpdateOne({"OP": op_id}, {"$set": doc}, upsert=True)
                )

        if operations:
            result = collection.bulk_write(operations)
            logging.info(f"Resultado Mongo: {result.upserted_count} nuevos, {result.modified_count} actualizados.")
        else:
            logging.info("No hay datos para cargar.")
    except Exception as e:
        logging.error(f"Error en carga a MongoDB: {e}")
    finally:
        if client: client.close()

def main():
    logging.info(f"--- Iniciando ETL (Rango {FILA_INICIO_DATOS} - {FILA_FIN_DATOS}) ---") 
    df_master = extract_master()
    df_plancor = extract_plancor()
    df_terminado = extract_terminado()
    
    if df_master is not None and df_plancor is not None and df_terminado is not None:
        df_transformado = transform(df_master, df_plancor, df_terminado)
        if df_transformado is not None and not df_transformado.empty:
            load(df_transformado)
        else:
            logging.warning("No hay datos válidos para cargar.")
    else:
        logging.error("Error en extracción de archivos.")

if __name__ == "__main__":
    main()