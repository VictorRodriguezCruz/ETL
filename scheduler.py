import pandas as pd
from sqlalchemy import create_engine, text, Date, cast
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta, date
import logging
import sys # Para manejar errores en la inicialización

# --- CONFIGURACIÓN (¡IMPORTANTE! DEBES AJUSTAR ESTO) ---

# 1. Conexión a tu base de datos PostgreSQL
DATABASE_URL = "postgresql://postgres:admin@localhost:5432/postgres"

# 2. Lógica de Negocio (Tus Reglas)
CAPACIDAD_DIARIA_DEFAULT = 180000.00
UMBRAL_CIERRE_DIA = 165000.00 # Si se llega a esto, se pasa al sig. día
VENTANA_DIAS_ENTREGA = 2 # Lunes considera hasta Miércoles (2 días)
DIAS_REPORTE_FUTUROS = 5 # El número de barras/días a mostrar (Lun-Vie)

# 3. Estatus que entran en la programación
ESTATUS_A_PROGRAMAR = [
    'SIN PROGRAMAR',
    'INGRESADO SIN PROGRAMAR',
    'PROGRAMADO PARCIAL',
    'SIN FABRICAR',
    'INGRESO' # El estatus de los datos crudos
]
# -----------------------------------------------------------------

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configuración de la Base de Datos
try:
    engine = create_engine(DATABASE_URL, connect_args={'client_encoding': 'latin1'})
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    logging.info("Motor de base de datos creado.")
except Exception as e:
    logging.error(f"Error fatal al crear el engine de BD: {e}")
    sys.exit(1) # Detiene el script si no se puede conectar


def obtener_reglas_calendario(session) -> dict:
    """Obtiene las reglas de capacidad y días festivos de la BD."""
    logging.info("Obteniendo reglas del calendario...")
    query = "SELECT fecha, es_laboral, capacidad_m2 FROM calendario"
    try:
        resultado = session.execute(text(query)).fetchall()
        # Convertimos la lista de tuplas en un diccionario para acceso rápido
        # ej: {'2025-11-07': (True, 400000.00), ...}
        reglas = {row[0]: (row[1], float(row[2])) for row in resultado}
        logging.info(f"Se cargaron {len(reglas)} reglas especiales del calendario.")
        return reglas
    except Exception as e:
        logging.error(f"Error al obtener calendario: {e}")
        return {}

def obtener_pedidos_para_programar(session) -> pd.DataFrame:
    """Obtiene los pedidos pendientes y los ordena por FIFO."""
    logging.info("Obteniendo pedidos pendientes de la BD...")
    try:
        query = text(f"""
            SELECT op, metros2_pedido, fecha_ingreso, fecha_entrega
            FROM pedidos
            WHERE 
                fecha_programacion_asignada IS NULL
            AND 
                estatus_excel IN :estatus_validos
            AND 
                bloqueado = FALSE
            ORDER BY 
                prioridad ASC, fecha_ingreso ASC;
        """)
        
        df_pedidos = pd.read_sql(query, session.connection(), params={"estatus_validos": tuple(ESTATUS_A_PROGRAMAR)})
        
        # Convertir tipos
        df_pedidos['metros2_pedido'] = pd.to_numeric(df_pedidos['metros2_pedido'], errors='coerce').fillna(0)
        df_pedidos['fecha_ingreso'] = pd.to_datetime(df_pedidos['fecha_ingreso'])
        df_pedidos['fecha_entrega'] = pd.to_datetime(df_pedidos['fecha_entrega'])
        
        logging.info(f"Se encontraron {len(df_pedidos)} pedidos para programar.")
        return df_pedidos
    except Exception as e:
        logging.error(f"Error al obtener pedidos: {e}")
        return pd.DataFrame()


def obtener_proximo_dia_habil(fecha_actual: date, reglas_calendario: dict) -> date:
    """Encuentra el próximo día laborable según las reglas."""
    siguiente_dia = fecha_actual + timedelta(days=1)
    
    while True:
        if siguiente_dia in reglas_calendario:
            es_laboral, _ = reglas_calendario[siguiente_dia]
            if es_laboral:
                return siguiente_dia # Es un día especial pero es laboral
        else:
            # Si no está en reglas, aplicar lógica de fin de semana
            # Lunes=0, Domingo=6
            if siguiente_dia.weekday() < 5: # 0-4 son Lunes a Viernes
                return siguiente_dia # Es un día de semana normal
        
        # Si no fue un día hábil, probamos con el siguiente
        siguiente_dia += timedelta(days=1)

def calcular_dias_reporte(reglas_calendario: dict) -> (list, date):
    """Calcula los 5 días del reporte y la fecha de "Posteriores"."""
    dias_reporte = []
    
    # Empezamos desde hoy
    dia_actual = datetime.now().date()
    
    # Si hoy es Sábado (5) o Domingo (6), empezamos desde el próximo Lunes
    if dia_actual.weekday() >= 5:
        dia_actual = obtener_proximo_dia_habil(dia_actual, reglas_calendario)
    
    # Verificamos si hoy (ej. Miércoles) es hábil
    if dia_actual in reglas_calendario:
        es_laboral, _ = reglas_calendario[dia_actual]
        if not es_laboral:
            dia_actual = obtener_proximo_dia_habil(dia_actual, reglas_calendario)
    
    # Llenamos los 5 días del reporte
    dias_reporte.append(dia_actual)
    while len(dias_reporte) < DIAS_REPORTE_FUTUROS:
        dia_actual = obtener_proximo_dia_habil(dia_actual, reglas_calendario)
        dias_reporte.append(dia_actual)
    
    # La fecha "Posteriores" es el siguiente día hábil después del último día
    fecha_posteriores = obtener_proximo_dia_habil(dias_reporte[-1], reglas_calendario)
    
    logging.info(f"Días de reporte calculados: {dias_reporte}")
    logging.info(f"Fecha de 'Posteriores' calculada: {fecha_posteriores}")
    
    return dias_reporte, fecha_posteriores


def calcular_fecha_limite_entrega(fecha_programacion: date, reglas_calendario: dict) -> date:
    """Calcula la ventana de entrega de 2 días hábiles (regla Viernes->Martes)."""
    dia_semana_prog = fecha_programacion.weekday()
    
    # Regla especial del Viernes (4) -> salta a Martes (1)
    if dia_semana_prog == 4: # Si estamos programando para un VIERNES
        fecha_limite = fecha_programacion + timedelta(days=4) # Martes
        # Nos aseguramos que ese martes sea hábil
        if fecha_limite in reglas_calendario:
             es_laboral, _ = reglas_calendario[fecha_limite]
             if not es_laboral:
                 fecha_limite = obtener_proximo_dia_habil(fecha_limite, reglas_calendario)
        return fecha_limite

    # Regla normal (Lunes a Jueves)
    dias_a_sumar = 0
    dia_actual = fecha_programacion
    while dias_a_sumar < VENTANA_DIAS_ENTREGA:
        dia_actual = obtener_proximo_dia_habil(dia_actual, reglas_calendario)
        dias_a_sumar += 1
    return dia_actual


def ejecutar_motor_programacion(session, df_pedidos, reglas_calendario):
    """Asigna fechas de programación a los pedidos usando la lógica de 5 días + posteriores."""
    logging.info("Iniciando motor de programación (v2.0)...")
    
    dias_reporte, fecha_posteriores = calcular_dias_reporte(reglas_calendario)
    
    # Diccionario para guardar las actualizaciones: {'OP-123': date(2025, 11, 4), ...}
    actualizaciones_fechas = {}
    
    # Diccionario para llevar la cuenta de la capacidad usada por día
    # {date(2025, 11, 3): 150000, ...}
    capacidad_usada_por_dia = {dia: 0.0 for dia in dias_reporte}
    capacidad_usada_por_dia[fecha_posteriores] = 0.0 # Capacidad ilimitada

    # Empezamos a programar desde el primer día del reporte
    dia_programacion_actual = dias_reporte[0] # Ej. Lunes

    # --- Bucle principal de asignación ---
    for _, pedido in df_pedidos.iterrows():
        op = pedido['op']
        m2 = pedido['metros2_pedido']
        
        dia_asignado = None
        dia_a_probar = dia_programacion_actual
        
        # --- Bucle interno: Encontrar un día para este pedido ---
        while True: 
            # Obtenemos la capacidad del día a probar
            regla_dia = reglas_calendario.get(dia_a_probar)
            capacidad_dia = regla_dia[1] if regla_dia else CAPACIDAD_DIARIA_DEFAULT
            
            # Obtenemos el uso actual de ese día
            uso_actual_dia = capacidad_usada_por_dia.get(dia_a_probar, 0)
            
            # Calculamos la fecha límite de entrega para esta "ventana"
            fecha_limite_ent = calcular_fecha_limite_entrega(dia_a_probar, reglas_calendario)

            # --- APLICACIÓN DE LÓGICA DE NEGOCIO ---

            # 1. ¿El pedido está en la ventana de entrega permitida?
            if pedido['fecha_entrega'].date() > fecha_limite_ent:
                # No. La fecha de entrega de este pedido es muy lejana para esta ventana.
                # Avanzamos la *ventana* al próximo día hábil.
                dia_a_probar = obtener_proximo_dia_habil(dia_a_probar, reglas_calendario)
                continue # Re-evaluamos el mismo pedido con la nueva ventana

            # 2. ¿El pedido cabe en la CAPACIDAD TOTAL del día a probar?
            if (uso_actual_dia + m2) <= capacidad_dia:
                # ¡Sí cabe!
                dia_asignado = dia_a_probar
                break # Salimos del bucle interno, ya encontramos día
            else:
                # No cabe. Este día está lleno.
                # Avanzamos al *próximo día hábil* para intentar meterlo ahí.
                dia_a_probar = obtener_proximo_dia_habil(dia_a_probar, reglas_calendario)
                continue # Re-evaluamos el mismo pedido en el nuevo día
        
        # --- Fin del bucle interno: Asignar el pedido ---
        
        # Si el día asignado está fuera de los 5 días, lo mandamos a "Posteriores"
        if dia_asignado not in dias_reporte:
            dia_asignado_final = fecha_posteriores
        else:
            dia_asignado_final = dia_asignado
            
        # Registramos la asignación
        actualizaciones_fechas[op] = dia_asignado_final
        capacidad_usada_por_dia[dia_asignado_final] += m2

        # 3. ¿El día asignado (no "Posteriores") superó el UMBRAL de cierre?
        if dia_asignado_final != fecha_posteriores and capacidad_usada_por_dia[dia_asignado_final] >= UMBRAL_CIERRE_DIA:
            # Sí. El próximo pedido debe empezar a buscar desde el *siguiente* día.
            dia_programacion_actual = obtener_proximo_dia_habil(dia_asignado_final, reglas_calendario)

    logging.info(f"Motor finalizado. Se asignaron fechas a {len(actualizaciones_fechas)} pedidos.")
    return actualizaciones_fechas, capacidad_usada_por_dia, dias_reporte, fecha_posteriores


def actualizar_base_datos(session, actualizaciones_fechas: dict):
    """Actualiza la tabla 'pedidos' con las fechas calculadas."""
    if not actualizaciones_fechas:
        logging.info("No hay actualizaciones de fechas para aplicar.")
        return

    logging.info(f"Actualizando {len(actualizaciones_fechas)} pedidos en la BD...")
    try:
        # Convertimos el diccionario a una lista de tuplas para el DF
        data_list = [{'op': op, 'fecha_prog_new': fecha} for op, fecha in actualizaciones_fechas.items()]
        df_updates = pd.DataFrame(data_list)
        
        # 2. Cargar a tabla temporal
        temp_table_name = "temp_fechas_prog"
        df_updates.to_sql(temp_table_name, session.connection(), if_exists='replace', index=False)
        
        # 3. Ejecutar UPDATE JOIN
        update_query = text(f"""
            UPDATE pedidos
            SET fecha_programacion_asignada = T.fecha_prog_new
            FROM {temp_table_name} AS T
            WHERE pedidos.op = T.op;
        """)
        session.execute(update_query)
        
        # 4. Borrar tabla temporal
        session.execute(text(f"DROP TABLE {temp_table_name}"))
        
        logging.info("Fechas de programación actualizadas en tabla 'pedidos'.")
        
    except Exception as e:
        logging.error(f"Error al actualizar fechas de pedidos: {e}")
        raise # Propagamos el error


def actualizar_reporte_capacidad(session, reglas_calendario, capacidad_usada, dias_reporte, fecha_posteriores):
    """Recalcula y actualiza la tabla 'reporte_capacidad_diaria'."""
    logging.info("Actualizando tabla 'reporte_capacidad_diaria'...")
    try:
        # 1. Borramos el reporte viejo
        session.execute(text("TRUNCATE TABLE reporte_capacidad_diaria"))
        
        # No consultamos la BD, usamos los datos que ya calculamos
        
        # 3. Preparamos los datos para insertar (los 5 días)
        datos_reporte = []
        for fecha in dias_reporte:
            regla = reglas_calendario.get(fecha)
            cap_total = regla[1] if regla else CAPACIDAD_DIARIA_DEFAULT
            m2_usados = capacidad_usada.get(fecha, 0.0)
            m2_disponibles = cap_total - m2_usados
            
            # Contamos los pedidos de ese día
            count_query = text("SELECT COUNT(op) FROM pedidos WHERE fecha_programacion_asignada = :fecha")
            num_pedidos = session.execute(count_query, {"fecha": fecha}).scalar_one()

            datos_reporte.append({
                "fecha": fecha,
                "capacidad_total_m2": cap_total,
                "m2_utilizados": m2_usados,
                "m2_disponibles": m2_disponibles,
                "conteo_pedidos": num_pedidos
            })
            
        # 4. Añadimos la barra "Posteriores"
        m2_usados_post = capacidad_usada.get(fecha_posteriores, 0.0)
        if m2_usados_post > 0:
            count_query_post = text("SELECT COUNT(op) FROM pedidos WHERE fecha_programacion_asignada = :fecha")
            num_pedidos_post = session.execute(count_query_post, {"fecha": fecha_posteriores}).scalar_one()
            
            datos_reporte.append({
                "fecha": fecha_posteriores,
                "capacidad_total_m2": m2_usados_post, # Capacidad total = m2 usados
                "m2_utilizados": m2_usados_post,
                "m2_disponibles": 0.0, # Siempre está "lleno"
                "conteo_pedidos": num_pedidos_post
            })

        # 5. Insertamos los nuevos datos del reporte
        if datos_reporte:
            insert_query = text("""
                INSERT INTO reporte_capacidad_diaria (fecha, capacidad_total_m2, m2_utilizados, m2_disponibles, conteo_pedidos)
                VALUES (:fecha, :capacidad_total_m2, :m2_utilizados, :m2_disponibles, :conteo_pedidos)
            """)
            session.execute(insert_query, datos_reporte)
        
        logging.info(f"Tabla 'reporte_capacidad_diaria' actualizada con {len(datos_reporte)} barras (días + posteriores).")
        
    except Exception as e:
        logging.error(f"Error al actualizar el reporte de capacidad: {e}")
        raise # Propagamos el error


def main():
    """Flujo principal del Motor de Programación."""
    logging.info("--- Iniciando Motor de Programación (v2.0 - 5 Días + Posteriores) ---")
    session = SessionLocal()
    
    try:
        # Obtenemos las reglas y los pedidos
        reglas_calendario = obtener_reglas_calendario(session)
        df_pedidos = obtener_pedidos_para_programar(session)
        
        if not df_pedidos.empty:
            # Corremos el motor
            actualizaciones, capacidad_usada, dias_rep, fecha_post = ejecutar_motor_programacion(session, df_pedidos, reglas_calendario)
            
            # Actualizamos la BD con los resultados
            actualizar_base_datos(session, actualizaciones)
            
            # Recalculamos el reporte de capacidad
            actualizar_reporte_capacidad(session, reglas_calendario, capacidad_usada, dias_rep, fecha_post)
            
        else:
            logging.info("No se encontraron pedidos nuevos para programar. Limpiando reporte antiguo.")
            session.execute(text("TRUNCATE TABLE reporte_capacidad_diaria"))
            
        # Si todo salió bien, confirmamos los cambios
        session.commit()
        logging.info("¡Motor de Programación finalizado con éxito! Cambios guardados.")
        
    except Exception as e:
        # Si algo falla, revertimos todo
        logging.error(f"¡Error! Revertiendo cambios... Detalle: {e}")
        session.rollback()
    finally:
        # Cerramos la sesión
        session.close()
        
    logging.info("--- Motor de Programación Finalizado ---")


if __name__ == "__main__":
    main()
