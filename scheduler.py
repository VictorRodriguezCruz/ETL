import pandas as pd
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta, date
import logging
import sys

# --- CONFIGURACIÓN ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "produccion_db"

CAPACIDAD_DIARIA_DEFAULT = 180000.00
UMBRAL_CIERRE_DIA = 165000.00 
VENTANA_DIAS_ENTREGA = 2 
DIAS_REPORTE_FUTUROS = 5 

# Estatus que consideramos "pendientes" para programar si no tienen fecha
ESTATUS_A_PROGRAMAR = [
    'SIN PROGRAMAR',
    'INGRESADO SIN PROGRAMAR',
    'PROGRAMADO PARCIAL',
    'SIN FABRICAR',
    'INGRESO' 
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- HELPERS ---
def limpiar_op(valor):
    if pd.isna(valor) or str(valor).strip() == '':
        return None
    try:
        val_float = float(valor)
        val_int = int(val_float)
        return str(val_int)
    except ValueError:
        return str(valor).strip()

def obtener_db():
    try:
        client = MongoClient(MONGO_URI)
        return client[DB_NAME]
    except Exception as e:
        logging.error(f"Error fatal al conectar a Mongo: {e}")
        sys.exit(1)

def obtener_reglas_calendario(db) -> dict:
    logging.info("Obteniendo reglas del calendario...")
    try:
        cursor = db.calendario.find({})
        reglas = {}
        for doc in cursor:
            fecha_val = doc.get('fecha')
            if isinstance(fecha_val, datetime):
                fecha_key = fecha_val.date()
            else:
                fecha_key = pd.to_datetime(fecha_val).date()
            reglas[fecha_key] = (
                doc.get('es_laboral', True), 
                float(doc.get('capacidad_m2', CAPACIDAD_DIARIA_DEFAULT))
            )
        return reglas
    except Exception as e:
        logging.error(f"Error al obtener calendario: {e}")
        return {}

# --- PASO 1: OBTENER SOLO LO NUEVO ---
def obtener_pedidos_para_programar(db) -> pd.DataFrame:
    logging.info("Obteniendo pedidos pendientes (sin fecha asignada)...")
    try:
        # El filtro CLAVE: Solo traemos lo que tiene fecha_programacion_asignada: null
        filtro = {
            "fecha_programacion_asignada": None, 
            "ESTATUS_EXCEL": {"$in": ESTATUS_A_PROGRAMAR}, 
            "bloqueado": {"$ne": True} 
        }
        proyeccion = {
            "OP": 1, "M2": 1, "FECHA_INGRESO": 1, "FECHA_ENTREGA": 1, "_id": 0, "prioridad": 1 
        }
        cursor = db.pedidos.find(filtro, proyeccion).sort([("prioridad", 1), ("FECHA_INGRESO", 1)])
        df_pedidos = pd.DataFrame(list(cursor))
        
        if not df_pedidos.empty:
            df_pedidos['M2'] = pd.to_numeric(df_pedidos['M2'], errors='coerce').fillna(0.0)
            df_pedidos['FECHA_INGRESO'] = pd.to_datetime(df_pedidos['FECHA_INGRESO'])
            df_pedidos['FECHA_ENTREGA'] = pd.to_datetime(df_pedidos['FECHA_ENTREGA'])
            df_pedidos['OP'] = df_pedidos['OP'].apply(limpiar_op)
            
        logging.info(f"Se encontraron {len(df_pedidos)} pedidos NUEVOS para programar.")
        return df_pedidos
    except Exception as e:
        logging.error(f"Error al obtener pedidos: {e}")
        return pd.DataFrame()

# --- LÓGICA DE FECHAS ---
def obtener_proximo_dia_habil(fecha_actual: date, reglas_calendario: dict) -> date:
    siguiente_dia = fecha_actual + timedelta(days=1)
    while True:
        if siguiente_dia in reglas_calendario:
            es_laboral, _ = reglas_calendario[siguiente_dia]
            if es_laboral: return siguiente_dia 
        else:
            if siguiente_dia.weekday() < 5: return siguiente_dia 
        siguiente_dia += timedelta(days=1)

def calcular_dias_reporte(reglas_calendario: dict) -> (list, date):
    dias_reporte = []
    dia_actual = datetime.now().date()
    
    if dia_actual.weekday() >= 5:
        dia_actual = obtener_proximo_dia_habil(dia_actual, reglas_calendario)
        
    dias_reporte.append(dia_actual)
    while len(dias_reporte) < DIAS_REPORTE_FUTUROS:
        dia_actual = obtener_proximo_dia_habil(dia_actual, reglas_calendario)
        dias_reporte.append(dia_actual)
    
    fecha_posteriores = obtener_proximo_dia_habil(dias_reporte[-1], reglas_calendario)
    return dias_reporte, fecha_posteriores

def calcular_fecha_limite_entrega(fecha_programacion: date, reglas_calendario: dict) -> date:
    dia_semana_prog = fecha_programacion.weekday()
    if dia_semana_prog == 4: 
        fecha_limite = fecha_programacion + timedelta(days=4) 
        if fecha_limite in reglas_calendario:
             es_laboral, _ = reglas_calendario[fecha_limite]
             if not es_laboral:
                 fecha_limite = obtener_proximo_dia_habil(fecha_limite, reglas_calendario)
        return fecha_limite

    dias_a_sumar = 0
    dia_actual = fecha_programacion
    while dias_a_sumar < VENTANA_DIAS_ENTREGA:
        dia_actual = obtener_proximo_dia_habil(dia_actual, reglas_calendario)
        dias_a_sumar += 1
    return dia_actual

# --- PASO 2: CALCULAR CARGA EXISTENTE (RESPETAR AL USUARIO) ---
def calcular_carga_previa(db, dias_reporte, fecha_posteriores):
    """
    Suma los M2 de pedidos que YA tienen fecha asignada en la BD.
    Esto incluye lo que el usuario movió manualmente (candados) y lo que ya se programó antes.
    """
    logging.info("Calculando carga ocupada por pedidos existentes...")
    capacidad_usada = {dia: 0.0 for dia in dias_reporte}
    capacidad_usada[fecha_posteriores] = 0.0
    
    # Buscamos todo lo que NO es nulo en fecha
    cursor = db.pedidos.find(
        {"fecha_programacion_asignada": {"$ne": None}}, 
        {"fecha_programacion_asignada": 1, "M2": 1}
    )
    
    for doc in cursor:
        fecha = doc.get("fecha_programacion_asignada")
        m2 = doc.get("M2", 0.0)
        
        if isinstance(fecha, datetime):
            fecha_date = fecha.date()
            if fecha_date in capacidad_usada:
                capacidad_usada[fecha_date] += m2
            elif fecha_date > dias_reporte[-1]:
                capacidad_usada[fecha_posteriores] += m2
                
    return capacidad_usada

# --- PASO 3: MOTOR DE PROGRAMACIÓN INTELIGENTE ---
def ejecutar_motor_programacion(db, df_pedidos, reglas_calendario):
    logging.info("Iniciando motor de programación...")
    dias_reporte, fecha_posteriores = calcular_dias_reporte(reglas_calendario)
    actualizaciones_fechas = {}
    
    # AQUI ESTA LA MAGIA: Iniciamos con el vaso medio lleno, no vacío
    capacidad_usada_por_dia = calcular_carga_previa(db, dias_reporte, fecha_posteriores)
    
    dia_programacion_actual = dias_reporte[0]

    # Avanzamos el día actual si ya está lleno por movimientos manuales previos
    while True:
        if dia_programacion_actual == fecha_posteriores: break
        
        regla = reglas_calendario.get(dia_programacion_actual)
        cap_total = regla[1] if regla else CAPACIDAD_DIARIA_DEFAULT
        usado = capacidad_usada_por_dia.get(dia_programacion_actual, 0)
        
        if usado >= UMBRAL_CIERRE_DIA:
            logging.info(f"Día {dia_programacion_actual} ya saturado ({usado:,.0f} m²). Buscando hueco en siguiente día...")
            nuevo_dia = obtener_proximo_dia_habil(dia_programacion_actual, reglas_calendario)
            if nuevo_dia not in dias_reporte:
                dia_programacion_actual = fecha_posteriores
                break
            dia_programacion_actual = nuevo_dia
        else:
            break

    # Asignamos los pedidos NUEVOS en los huecos libres
    for _, pedido in df_pedidos.iterrows():
        op = pedido['OP'] 
        m2 = pedido['M2'] 
        
        dia_asignado = None
        dia_a_probar = dia_programacion_actual
        
        while True: 
            # Si llegamos a posteriores, ahí se queda
            if dia_a_probar == fecha_posteriores:
                dia_asignado = fecha_posteriores
                break
                
            regla_dia = reglas_calendario.get(dia_a_probar)
            capacidad_dia = regla_dia[1] if regla_dia else CAPACIDAD_DIARIA_DEFAULT
            uso_actual_dia = capacidad_usada_por_dia.get(dia_a_probar, 0)
            
            # Validar fecha entrega (Business Logic)
            fecha_limite_ent = calcular_fecha_limite_entrega(dia_a_probar, reglas_calendario)
            if pedido['FECHA_ENTREGA'].date() > fecha_limite_ent:
                dia_a_probar = obtener_proximo_dia_habil(dia_a_probar, reglas_calendario)
                if dia_a_probar not in dias_reporte: dia_a_probar = fecha_posteriores
                continue 
            
            # Validar capacidad (Respetando lo manual)
            if (uso_actual_dia + m2) <= capacidad_dia:
                dia_asignado = dia_a_probar
                break 
            else:
                dia_a_probar = obtener_proximo_dia_habil(dia_a_probar, reglas_calendario)
                if dia_a_probar not in dias_reporte: dia_a_probar = fecha_posteriores
                continue 
        
        actualizaciones_fechas[op] = dia_asignado
        capacidad_usada_por_dia[dia_asignado] += m2

        # Si llenamos el día actual con automáticos, avanzamos
        if dia_asignado == dia_programacion_actual and dia_asignado != fecha_posteriores:
             if capacidad_usada_por_dia[dia_asignado] >= UMBRAL_CIERRE_DIA:
                nuevo_dia = obtener_proximo_dia_habil(dia_asignado, reglas_calendario)
                if nuevo_dia in dias_reporte:
                    dia_programacion_actual = nuevo_dia
                else:
                    dia_programacion_actual = fecha_posteriores

    return actualizaciones_fechas, capacidad_usada_por_dia, dias_reporte, fecha_posteriores

def actualizar_base_datos(db, actualizaciones_fechas: dict):
    if not actualizaciones_fechas: return
    logging.info(f"Guardando fechas de {len(actualizaciones_fechas)} pedidos nuevos...")
    try:
        operations = []
        for op, fecha_date in actualizaciones_fechas.items():
            fecha_iso = datetime.combine(fecha_date, datetime.min.time())
            op_limpia = limpiar_op(op)
            # Solo actualizamos la fecha, NO ponemos candado (fijo_usuario) porque es automático
            operations.append(UpdateOne({"OP": op_limpia}, {"$set": {"fecha_programacion_asignada": fecha_iso}}))
        
        if operations:
            result = db.pedidos.bulk_write(operations)
            logging.info(f"Fechas asignadas automáticamente: {result.modified_count}")
    except Exception as e:
        logging.error(f"Error al actualizar MongoDB: {e}")
        raise 

def actualizar_reporte_capacidad(db, reglas_calendario, capacidad_usada, dias_reporte, fecha_posteriores):
    logging.info("Regenerando reporte de capacidad (agregando lo manual + automático)...")
    try:
        db.reporte_capacidad_diaria.delete_many({})
        datos_reporte = []
        
        # Hacemos una agregación DIRECTA en base de datos para tener la verdad absoluta
        # (Suma lo que acabamos de guardar + lo que ya existía)
        for fecha in dias_reporte:
            regla = reglas_calendario.get(fecha)
            cap_total = regla[1] if regla else CAPACIDAD_DIARIA_DEFAULT
            
            fecha_iso = datetime.combine(fecha, datetime.min.time())
            
            pipeline = [
                {"$match": {"fecha_programacion_asignada": fecha_iso}},
                {"$group": {"_id": None, "total_m2": {"$sum": "$M2"}, "conteo": {"$sum": 1}}}
            ]
            res = list(db.pedidos.aggregate(pipeline))
            m2_reales = res[0]['total_m2'] if res else 0.0
            conteo = res[0]['conteo'] if res else 0
            
            datos_reporte.append({
                "fecha": fecha_iso, 
                "capacidad_total_m2": cap_total, 
                "m2_utilizados": m2_reales, 
                "m2_disponibles": cap_total - m2_reales, 
                "conteo_pedidos": conteo
            })
            
        # Calcular "Posteriores" (Todo lo que cae después del horizonte visible)
        fecha_limite_horizonte = datetime.combine(dias_reporte[-1], datetime.max.time())
        
        pipeline_post = [
             {"$match": {"fecha_programacion_asignada": {"$gt": fecha_limite_horizonte}}},
             {"$group": {"_id": None, "total_m2": {"$sum": "$M2"}, "conteo": {"$sum": 1}}}
        ]
        res_post = list(db.pedidos.aggregate(pipeline_post))
        m2_post = res_post[0]['total_m2'] if res_post else 0.0
        conteo_post = res_post[0]['conteo'] if res_post else 0
        
        if m2_post > 0:
            # Guardamos "Posteriores" con la fecha real del objeto fecha_posteriores
            # El backend (main.py) se encargará de agruparlo visualmente si es necesario
            fecha_post_obj = datetime.combine(fecha_posteriores, datetime.min.time())
            
            datos_reporte.append({
                "fecha": fecha_post_obj, 
                "capacidad_total_m2": m2_post, 
                "m2_utilizados": m2_post, 
                "m2_disponibles": 0.0, 
                "conteo_pedidos": conteo_post
            })

        if datos_reporte:
            db.reporte_capacidad_diaria.insert_many(datos_reporte)
        logging.info("Reporte actualizado correctamente.")
    except Exception as e:
        logging.error(f"Error al actualizar el reporte: {e}")
        raise 

def main():
    logging.info("--- Iniciando Scheduler Inteligente (Modo Respeto) ---")
    db = obtener_db()
    try:
        reglas_calendario = obtener_reglas_calendario(db)
        
        # 1. Obtener SOLO lo que NO tiene fecha (Pedidos Nuevos)
        df_pedidos = obtener_pedidos_para_programar(db)
        
        # 2. Ejecutar motor (Pasamos df vacio si no hay nuevos, solo para recalcular reporte)
        actualizaciones, capacidad_usada, dias_rep, fecha_post = ejecutar_motor_programacion(db, df_pedidos, reglas_calendario)
            
        # 3. Guardar fechas de pedidos nuevos
        if not df_pedidos.empty:
            actualizar_base_datos(db, actualizaciones)
        
        # 4. Regenerar reporte final
        actualizar_reporte_capacidad(db, reglas_calendario, capacidad_usada, dias_rep, fecha_post)
            
        logging.info("¡Scheduler finalizado!")
    except Exception as e:
        logging.error(f"¡Error crítico!: {e}")

if __name__ == "__main__":
    main()