from fastapi import FastAPI, HTTPException, Body
from pymongo import MongoClient, UpdateOne
from datetime import datetime, time, timedelta
import logging
import traceback
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

# --- CONFIGURACIÓN ---
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "produccion_db"
CAPACIDAD_DIARIA_DEFAULT = 180000.00 
LIMITE_CAPACIDAD_CON_TOLERANCIA = 190000.00  # 180k + 10k tolerancia

# --- INICIALIZACIÓN ---
app = FastAPI(title="API de Programación Pycapsa (Mongo)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    logging.info("Conexión a MongoDB establecida.")
except Exception as e:
    logging.error(f"Error al conectar a MongoDB: {e}")
    exit()

# --- MODELOS ---
class SwapRequest(BaseModel):
    ops_origen: list[str]
    ops_destino: list[str]
    fecha_origen: str
    fecha_destino: str

# --- HELPER PARA ACTUALIZAR GRÁFICA (REPORTES) ---
def recalcular_capacidad_dia(fecha_dt):
    """
    Recalcula y actualiza el documento en reporte_capacidad_diaria para una fecha específica.
    """
    try:
        fecha_iso = datetime.combine(fecha_dt.date(), time.min)
        
        # 1. Obtener capacidad total (Buscamos en calendario o usamos default)
        calendario_doc = db.calendario.find_one({"fecha": fecha_iso})
        capacidad_total = calendario_doc.get('capacidad_m2', CAPACIDAD_DIARIA_DEFAULT) if calendario_doc else CAPACIDAD_DIARIA_DEFAULT
        
        # 2. Sumar pedidos asignados a esa fecha
        pipeline = [
            {"$match": {"fecha_programacion_asignada": fecha_iso}},
            {"$group": {"_id": None, "total_m2": {"$sum": "$M2"}, "conteo": {"$sum": 1}}}
        ]
        resultado = list(db.pedidos.aggregate(pipeline))
        
        m2_utilizados = resultado[0]['total_m2'] if resultado else 0.0
        conteo_pedidos = resultado[0]['conteo'] if resultado else 0
        m2_disponibles = capacidad_total - m2_utilizados

        # 3. Actualizar colección de reporte
        db.reporte_capacidad_diaria.update_one(
            {"fecha": fecha_iso},
            {"$set": {
                "capacidad_total_m2": capacidad_total,
                "m2_utilizados": m2_utilizados,
                "m2_disponibles": m2_disponibles,
                "conteo_pedidos": conteo_pedidos
            }},
            upsert=True
        )
        logging.info(f"♻️ Reporte actualizado para {fecha_iso.date()}")
    except Exception as e:
        logging.error(f"Error recalculando capacidad para {fecha_dt}: {e}")


# --- ENDPOINTS ---

@app.get("/")
def read_root():
    return {"mensaje": "API Activa V5 - Fix ObjectId"}

@app.get("/api/reporte-capacidad") 
def get_reporte_capacidad():
    try:
        # 1. Definir el Horizonte (Hoy + 5 días)
        hoy = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        dias_visibles = 5
        fecha_corte = hoy + timedelta(days=dias_visibles)

        # 2. Obtener días dentro del horizonte (Detallado)
        pipeline_cercanos = [
            {
                "$match": {
                    "fecha": {"$gte": hoy, "$lte": fecha_corte}
                }
            },
            {"$sort": {"fecha": 1}},
            {"$project": {"_id": 0}} # <--- CORRECCIÓN CLAVE: Eliminamos el _id que causa el error 500
        ]
        reporte_cercano = list(db.reporte_capacidad_diaria.aggregate(pipeline_cercanos))

        # 3. Obtener todo lo posterior al horizonte (Agrupado)
        pipeline_lejanos = [
            {
                "$match": {
                    "fecha": {"$gt": fecha_corte}
                }
            },
            {
                "$group": {
                    "_id": "posteriores",
                    "m2_utilizados": {"$sum": "$m2_utilizados"},
                    "capacidad_total_m2": {"$sum": "$capacidad_total_m2"},
                    "conteo_pedidos": {"$sum": "$conteo_pedidos"}
                }
            }
        ]
        resultado_lejanos = list(db.reporte_capacidad_diaria.aggregate(pipeline_lejanos))

        # 4. Combinar resultados
        data_final = reporte_cercano

        if resultado_lejanos:
            agregado = resultado_lejanos[0]
            m2_total = agregado.get('capacidad_total_m2', 0)
            m2_usados = agregado.get('m2_utilizados', 0)
            
            # Creamos objeto ficticio para la barra "Posteriores"
            objeto_posteriores = {
                "fecha": "Posteriores", 
                "capacidad_total_m2": m2_total,
                "m2_utilizados": m2_usados,
                "m2_disponibles": 0, 
                "conteo_pedidos": agregado.get('conteo_pedidos', 0)
            }
            data_final.append(objeto_posteriores)

        return JSONResponse(content=jsonable_encoder(data_final))

    except Exception as e:
        logging.error(f"Error en reporte: {e}")
        return JSONResponse(content=[], status_code=500)

@app.get("/api/pedidos/{fecha_str}") 
def get_pedidos_por_fecha(fecha_str: str):
    try:
        # Manejo especial para la barra "Posteriores"
        if fecha_str == "Posteriores":
            hoy = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            dias_visibles = 5
            fecha_corte = hoy + timedelta(days=dias_visibles)
            
            cursor = db.pedidos.find(
                {"fecha_programacion_asignada": {"$gt": fecha_corte}}, 
                {'_id': 0} 
            ).sort([("fecha_programacion_asignada", 1)])
            return JSONResponse(content=jsonable_encoder(list(cursor)))

        try:
            fecha_obj = datetime.strptime(fecha_str, "%Y-%m-%d")
        except ValueError:
            return JSONResponse(content={"error": "Fecha inválida"}, status_code=400)

        fecha_busqueda = datetime.combine(fecha_obj.date(), time.min)
        
        cursor = db.pedidos.find(
            {"fecha_programacion_asignada": fecha_busqueda}, 
            {'_id': 0} 
        ).sort([("prioridad", 1), ("FECHA_INGRESO", 1)])
        
        return JSONResponse(content=jsonable_encoder(list(cursor)))

    except Exception as e:
        return JSONResponse(content={"detail": str(e)}, status_code=500)

@app.get("/api/todos-los-pedidos")
def get_all_pedidos(skip: int = 0, limit: int = 1000, buscar: str = None):
    """
    Retorna un listado paginado de pedidos.
    - skip: Cuántos registros saltar (para paginación).
    - limit: Cuántos registros traer (default 1000 para no saturar).
    - buscar: (Opcional) Filtra por OP o Cliente.
    """
    try:
        filtro = {}
        
        # Si el usuario envía un texto para buscar
        if buscar:
            # Crea una búsqueda insensible a mayúsculas/minúsculas en OP o CLIENTE
            regex_search = {"$regex": buscar, "$options": "i"}
            filtro = {
                "$or": [
                    {"OP": regex_search},
                    {"CLIENTE": regex_search}
                ]
            }

        # Consulta a la base de datos
        # Proyectamos {'_id': 0} para evitar errores de serialización
        cursor = db.pedidos.find(filtro, {'_id': 0})\
            .sort([("prioridad", 1), ("OP", 1)])\
            .skip(skip)\
            .limit(limit)
            
        lista_pedidos = list(cursor)
        
        # Información extra para saber si hay más datos
        total_coincidencias = db.pedidos.count_documents(filtro)

        return JSONResponse(content={
            "data": jsonable_encoder(lista_pedidos),
            "total": total_coincidencias,
            "skip": skip,
            "limit": limit
        })

    except Exception as e:
        logging.error(f"Error obteniendo listado completo: {e}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


@app.post("/api/pedidos/intercambiar")
def intercambiar_pedidos(payload: SwapRequest):
    logging.info(f"⚡ Swap solicitado: {len(payload.ops_origen)} (Origen) vs {len(payload.ops_destino)} (Destino)")
    
    try:
        # 1. Preparar Fechas
        fecha_origen_dt = datetime.strptime(payload.fecha_origen, "%Y-%m-%d")
        fecha_destino_dt = datetime.strptime(payload.fecha_destino, "%Y-%m-%d")
        fecha_destino_iso = datetime.combine(fecha_destino_dt.date(), time.min)

        # 2. Obtener Documentos de los pedidos involucrados (Calculo Real)
        pedidos_origen_docs = list(db.pedidos.find({"OP": {"$in": payload.ops_origen}}))
        pedidos_destino_docs = list(db.pedidos.find({"OP": {"$in": payload.ops_destino}}))

        m2_entrando_a_destino = sum(p.get("M2", 0) for p in pedidos_origen_docs) 
        m2_saliendo_de_destino = sum(p.get("M2", 0) for p in pedidos_destino_docs)

        # 3. --- VALIDACIÓN CRÍTICA: ESTADO ACTUAL DEL DESTINO ---
        # Consultamos cuánto tiene cargado el día destino ACTUALMENTE en la base de datos
        pipeline_carga = [
            {"$match": {"fecha_programacion_asignada": fecha_destino_iso}},
            {"$group": {"_id": None, "total_m2": {"$sum": "$M2"}}}
        ]
        resultado_carga = list(db.pedidos.aggregate(pipeline_carga))
        carga_actual_destino = resultado_carga[0]['total_m2'] if resultado_carga else 0.0

        # Cálculo de la proyección final
        carga_final_proyectada = carga_actual_destino + m2_entrando_a_destino - m2_saliendo_de_destino

        logging.info(f"🛡️ Validación: Actual({carga_actual_destino:.0f}) + Entra({m2_entrando_a_destino:.0f}) - Sale({m2_saliendo_de_destino:.0f}) = Final({carga_final_proyectada:.0f})")

        if carga_final_proyectada > LIMITE_CAPACIDAD_CON_TOLERANCIA:
            msg = f"⛔ IMPOSIBLE: El día destino quedaría con {carga_final_proyectada:,.0f} m². El límite es {LIMITE_CAPACIDAD_CON_TOLERANCIA:,.0f} m²."
            return JSONResponse(content={"success": False, "message": msg}, status_code=400)

        # 4. Si pasa la validación, Ejecutar Swap con CANDADO (fijo_usuario=True)
        operations = []
        
        # Mover Origen -> Destino (CON CANDADO)
        for op in payload.ops_origen:
            operations.append(UpdateOne(
                {"OP": op}, 
                {"$set": {
                    "fecha_programacion_asignada": fecha_destino_dt,
                    "fijo_usuario": True 
                }}
            ))
        
        # Mover Destino -> Origen (CON CANDADO)
        for op in payload.ops_destino:
            operations.append(UpdateOne(
                {"OP": op}, 
                {"$set": {
                    "fecha_programacion_asignada": fecha_origen_dt,
                    "fijo_usuario": True 
                }}
            ))

        if operations:
            db.pedidos.bulk_write(operations)
            
            # Recalcular ambas gráficas
            recalcular_capacidad_dia(fecha_origen_dt)
            recalcular_capacidad_dia(fecha_destino_dt)

            return JSONResponse(content={
                "success": True, 
                "message": f"Cambio exitoso. Destino quedó en {carga_final_proyectada:,.0f} m².",
            })
        else:
            return JSONResponse(content={"success": False, "message": "No hay OPs para mover"}, status_code=400)

    except Exception as e:
        logging.error(traceback.format_exc())
        return JSONResponse(content={"success": False, "message": str(e)}, status_code=500)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)