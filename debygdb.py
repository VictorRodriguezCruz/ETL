from pymongo import MongoClient
import pandas as pd

# Configuración
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "produccion_db"

def inspeccionar_db():
    print("--- INSPECCIÓN DE BASE DE DATOS ---")
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        
        # 1. Ver un pedido programado cualquiera
        print("\n1. Buscando un pedido programado al azar:")
        ejemplo = db.pedidos.find_one({"fecha_programacion_asignada": {"$ne": None}})
        
        if ejemplo:
            print(f"   OP: {ejemplo.get('OP', 'N/A')}")
            fecha_mongo = ejemplo.get('fecha_programacion_asignada')
            print(f"   Fecha en Mongo (Raw): {fecha_mongo}")
            print(f"   Tipo de dato: {type(fecha_mongo)}")
        else:
            print("   ¡ALERTA! No hay ningún pedido con fecha asignada en la BD.")

        # 2. Contar pedidos para el 18 de Noviembre (Prueba dura)
        print("\n2. Prueba de conteo para 2025-11-18:")
        
        from datetime import datetime
        target_date = datetime(2025, 11, 18) # Naive
        count = db.pedidos.count_documents({"fecha_programacion_asignada": target_date})
        print(f"   Búsqueda exacta (Naive 00:00:00): {count} encontrados.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    inspeccionar_db()