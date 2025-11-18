import pandas as pd
from sqlalchemy import create_engine, text
import numpy as np
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta

# --- CONFIGURACIÓN ---
DATABASE_URL = "postgresql://postgres:admin@localhost:5432/postgres"
PATH_BASE = r"C:\Users\Vick\Desktop\ProyectoETL"
FILE_MASTER_LIST = fr"{PATH_BASE}\COMPROBACION ERRORES BUENO.xlsm"
FILE_PLANCOR = fr"{PATH_BASE}\PEDIDOS.xlsx"
FILE_TERMINADO = fr"{PATH_BASE}\PRODUCTO TERMINADO.xlsx"

SHEET_MASTER_LIST = "INGRESO DE ORDENES"
SHEET_PLANCOR = 0
SHEET_TERMINADO = 0

COLUMNS_MAP = {
    "NotaPedido": "op",
    "CLIENTE": "cliente",
    "TipoProd": "tipo",
    "TipoMat": "resistencia",
    "Flauta": "flauta",
    "Ancho": "ancho",
    "Largo": "largo",
    "Fecha de Ingreso": "fecha_ingreso",
    "CantPed": "cantidad",
    "Fecha Entrega": "fecha_entrega",
    "O/C": "oc"
}

CLIENTES_EXCLUIDOS = [
    'PYCAPSA TRIM', 
    'BOX NOW', 
    'MUESTRAS'
]
# -----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_master() -> pd.DataFrame | None:
    logging.info(f"Extrayendo MAESTRA desde: {FILE_MASTER_LIST} (Hoja: {SHEET_MASTER_LIST})")
    try:
        df = pd.read_excel(
            FILE_MASTER_LIST,
            sheet_name=SHEET_MASTER_LIST,
            engine='openpyxl',
            header=2
        )
        df.columns = df.columns.str.strip()
        logging.info(f"Se leyeron {len(df)} filas de la MAESTRA (antes de filtrar).")
        return df
    except Exception as e:
        logging.error(f"Error al leer la MAESTRA: {e}")
        return None

def extract_plancor() -> pd.DataFrame | None:
    logging.info(f"Extrayendo PLANCOR desde: {FILE_PLANCOR}")
    try:
        df = pd.read_excel(
            FILE_PLANCOR,
            sheet_name=SHEET_PLANCOR,
            usecols="A,AY",
            engine='openpyxl'
        )
        df.columns = ['op_plancor', 'cantidad_plancor']
        df['op_plancor'] = df['op_plancor'].astype(str)
        logging.info(f"Se leyeron {len(df)} filas de PLANCOR.")
        return df
    except Exception as e:
        logging.error(f"Error al leer PLANCOR: {e}")
        return None

def extract_terminado() -> pd.DataFrame | None:
    logging.info(f"Extrayendo TERMINADO desde: {FILE_TERMINADO}")
    try:
        df = pd.read_excel(
            FILE_TERMINADO,
            sheet_name=SHEET_TERMINADO,
            usecols="A",
            engine='openpyxl'
        )
        df.columns = ['op_terminado']
        df.dropna(inplace=True)
        df['op_terminado'] = df['op_terminado'].astype(str)
        df['existe_en_terminado'] = True
        df.drop_duplicates(subset=['op_terminado'], inplace=True)
        logging.info(f"Se leyeron {len(df)} filas únicas de TERMINADO.")
        return df
    except Exception as e:
        logging.error(f"Error al leer TERMINADO: {e}")
        return None


def transform(df_master, df_plancor, df_terminado) -> pd.DataFrame | None:
    logging.info("Iniciando transformación de datos...")
    
    # --- 1. Limpieza de la Hoja Maestra ---
    columnas_requeridas = list(COLUMNS_MAP.keys())
    
    columnas_faltantes = [col for col in columnas_requeridas if col not in df_master.columns]
    if columnas_faltantes:
        logging.error(f"Error: Faltan columnas en el Excel (después de limpiar): {columnas_faltantes}")
        return None

    df_master = df_master[columnas_requeridas].copy()
    df_master.rename(columns=COLUMNS_MAP, inplace=True)
    df_master.dropna(subset=['op'], inplace=True)
    df_master['op'] = df_master['op'].astype(str)
    
    # --- ¡CAMBIO A ENTEROS! ---
    df_master['ancho'] = pd.to_numeric(df_master['ancho'], errors='coerce').fillna(0).astype(int)
    df_master['largo'] = pd.to_numeric(df_master['largo'], errors='coerce').fillna(0).astype(int)
    df_master['cantidad'] = pd.to_numeric(df_master['cantidad'], errors='coerce').fillna(0).astype(int)
    
    df_master['fecha_ingreso'] = pd.to_datetime(df_master['fecha_ingreso'], errors='coerce')
    df_master['fecha_entrega'] = pd.to_datetime(df_master['fecha_entrega'], errors='coerce')
    
    # --- Filtros de Fechas y Clientes (Tus reglas) ---
    df_master.dropna(subset=['fecha_ingreso', 'fecha_entrega'], inplace=True)
    
    logging.info("Filtrando fechas... (últimos 3 meses)")
    hoy = datetime.now()
    fecha_corte = (hoy - relativedelta(months=2)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    df_master = df_master[df_master['fecha_ingreso'] >= fecha_corte].copy()
    logging.info(f"Se encontraron {len(df_master)} pedidos en el rango de fechas.")

    df_master.dropna(subset=['cliente'], inplace=True)
    logging.info(f"Filas después de quitar clientes nulos: {len(df_master)}")
    
    df_master = df_master[~df_master['cliente'].isin(CLIENTES_EXCLUIDOS)].copy()
    logging.info(f"Filas después de excluir clientes: {len(df_master)}")

    df_master['metros2_pedido'] = (df_master['ancho'] * df_master['largo'] * df_master['cantidad']) / 1_000_000
    
    df_master['cliente'] = df_master['cliente'].fillna('SIN CLIENTE')
    df_master['oc'] = df_master['oc'].fillna('SIN O/C') 
    df_master['tipo'] = df_master['tipo'].fillna('SIN TIPO')
    df_master['resistencia'] = df_master['resistencia'].fillna('SIN DATO')
    df_master['flauta'] = df_master['flauta'].fillna('SIN DATO')
    
    # --- 2. Replicar VLOOKUP/XLOOKUP ---
    logging.info("Combinando (merge) datos de Plancor y Terminado...")
    df = pd.merge(df_master, df_plancor, left_on='op', right_on='op_plancor', how='left')
    df = pd.merge(df, df_terminado, left_on='op', right_on='op_terminado', how='left')

    # --- 3. Replicar Lógica de Estatus ---
    logging.info("Calculando estatus (replicando lógica de macro)...")
    
    conditions = [
        (df['cantidad_plancor'].isna()),
        (df['cantidad_plancor'] == 0),
        (df['cantidad_plancor'] < (df['cantidad'] * 0.9)),
        (df['cantidad_plancor'] >= (df['cantidad'] * 0.9))
    ]
    choices = [ "SIN PROGRAMAR", "INGRESADO SIN PROGRAMAR", "PROGRAMADO PARCIAL", "PROGRAMADO" ]
    df['estatus_plancor'] = np.select(conditions, choices, default='ERROR')
    df['estatus_excel'] = np.where(
        (df['estatus_plancor'] == 'SIN PROGRAMAR') & (df['existe_en_terminado'].isna()),
        "SIN FABRICAR",
        df['estatus_plancor']
    )
    
    # --- 4. Filtro final de estatus ---
    logging.info("Filtrando pedidos con estatus 'PROGRAMADO'...")
    df = df[df['estatus_excel'] != 'PROGRAMADO'].copy()
    logging.info(f"Filas después de excluir 'PROGRAMADO': {len(df)}")

    # --- ¡AQUÍ ESTÁ LA CORRECCIÓN! ---
    # Ya no añadimos 'resistencia' y 'flauta' al final,
    # porque ya están incluidas en COLUMNS_MAP.values()
    columnas_bd = list(COLUMNS_MAP.values()) + ['metros2_pedido', 'estatus_excel']
    df_final = df[columnas_bd]
    
    logging.info(f"Transformación completada. {len(df_final)} filas listas para cargar.")
    return df_final


def load(df: pd.DataFrame, engine):
    logging.info("Iniciando carga a PostgreSQL...")
    temp_table = "temp_pedidos_carga"
    try:
        with engine.begin() as conn:
            df.to_sql(temp_table, conn, if_exists='replace', index=False)
            columnas = list(df.columns)
            data_columns = [col for col in columnas if col != 'op']
            update_set = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in data_columns])

            upsert_sql = f"""
                INSERT INTO pedidos ({", ".join([f'"{c}"' for c in columnas])})
                SELECT {", ".join([f'"{c}"' for c in columnas])} FROM {temp_table}
                ON CONFLICT (op) DO UPDATE
                SET {update_set};
            """
            conn.execute(text(upsert_sql))
            conn.execute(text(f"DROP TABLE {temp_table}"))
            logging.info("¡CARGA COMPLETADA CON ÉXITO!")
    except Exception as e:
        logging.error(f"Error DURANTE LA CARGA a PostgreSQL: {e}")


def main():
    logging.info("--- Iniciando Proceso ETL de Pedidos (V3.1 - Corrección Duplicados) ---") 
    
    df_master = extract_master()
    df_plancor = extract_plancor()
    df_terminado = extract_terminado()
    
    if df_master is not None and df_plancor is not None and df_terminado is not None:
        df_transformado = transform(df_master, df_plancor, df_terminado)
        
        if df_transformado is not None and not df_transformado.empty:
            try:
                db_params = {'client_encoding': 'latin1'}
                engine = create_engine(DATABASE_URL, connect_args=db_params)
                
                with engine.connect() as conn_test:
                    logging.info("¡Conexión a la base de datos PostgreSQL exitosa!")

                load(df_transformado, engine)
            except Exception as e:
                logging.error(f"Error al CONECTAR o cargar: {e}")
        else:
            logging.warning("No hay datos transformados para cargar.")
    else:
        logging.error("Extracción fallida (faltan archivos). Terminando proceso.")
        
    logging.info("--- Proceso ETL de Pedidos (V3.1) Finalizado ---")


if __name__ == "__main__":
    main()

