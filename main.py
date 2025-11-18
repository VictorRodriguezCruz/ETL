import pandas as pd
from sqlalchemy import create_engine, text, Column, String, Integer, Date, Numeric, Boolean, TIMESTAMP
from sqlalchemy.orm import sessionmaker, declarative_base
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date
import logging
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from decimal import Decimal

# --- CONFIGURACIÓN ---
DATABASE_URL = "postgresql://postgres:admin@localhost:5432/postgres"

# --- INICIALIZACIÓN DE FASTAPI Y BASE DE DATOS ---
app = FastAPI(title="API de Programación Pycapsa")

# Configuración de CORS
origins = [
    "http://localhost",
    "http://localhost:3000",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuración de SQLAlchemy
try:
    engine = create_engine(DATABASE_URL, connect_args={'client_encoding': 'latin1'})
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base = declarative_base() 
    logging.info("Motor de base de datos para API creado.")
except Exception as e:
    logging.error(f"Error al crear el engine de BD: {e}")
    exit()

# --- MODELOS DE LA BASE DE DATOS (SQLAlchemy) ---
class ReporteCapacidadDiaria(Base):
    __tablename__ = "reporte_capacidad_diaria"
    __table_args__ = {'extend_existing': True} 
    fecha = Column(Date, primary_key=True)
    capacidad_total_m2 = Column(Numeric)
    m2_utilizados = Column(Numeric)
    m2_disponibles = Column(Numeric)
    conteo_pedidos = Column(Integer)

# ¡MODELO ACTUALIZADO!
class Pedido(Base):
    __tablename__ = "pedidos"
    __table_args__ = {'extend_existing': True}
    op = Column(String, primary_key=True)
    oc = Column(String, nullable=True)
    cliente = Column(String, nullable=True)
    tipo = Column(String, nullable=True)
    resistencia = Column(String, nullable=True) # <-- NUEVO
    flauta = Column(String, nullable=True)      # <-- NUEVO
    ancho = Column(Integer, nullable=True)      # <-- TIPO CAMBIADO
    largo = Column(Integer, nullable=True)      # <-- TIPO CAMBIADO
    cantidad = Column(Integer, nullable=True)   # <-- NUEVO
    metros2_pedido = Column(Numeric)
    fecha_ingreso = Column(Date)                # <-- TIPO CAMBIADO
    fecha_entrega = Column(Date)
    estatus_excel = Column(String, nullable=True)
    fecha_programacion_asignada = Column(Date, nullable=True)
    prioridad = Column(Integer, default=10)

# --- ESQUEMAS DE LA API (Pydantic) ---
class ReporteDiarioSchema(BaseModel):
    fecha: date
    capacidad_total_m2: float
    m2_utilizados: float
    m2_disponibles: float
    conteo_pedidos: int

    class Config:
        from_attributes = True
        json_encoders = { Decimal: float }

# ¡ESQUEMA ACTUALIZADO!
class PedidoSchema(BaseModel):
    op: str
    oc: Optional[str] = None
    cliente: Optional[str] = None
    resistencia: Optional[str] = None    # <-- NUEVO
    flauta: Optional[str] = None         # <-- NUEVO
    ancho: Optional[int] = None          # <-- NUEVO
    largo: Optional[int] = None          # <-- NUEVO
    cantidad: Optional[int] = None       # <-- NUEVO
    metros2_pedido: float
    fecha_ingreso: date                  # <-- TIPO CAMBIADO
    fecha_entrega: date
    estatus_excel: Optional[str] = None
    fecha_programacion_asignada: Optional[date] = None
    prioridad: Optional[int] = None
    
    class Config:
        from_attributes = True
        json_encoders = { Decimal: float }

# --- DEPENDENCIA DE BASE DE DATOS ---
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- ENDPOINTS DE LA API ---
@app.get("/")
def read_root():
    return {"mensaje": "¡Bienvenido a la API de Programación!"}

@app.get("/api/reporte-capacidad", response_model=List[ReporteDiarioSchema])
def get_reporte_capacidad(db: Session = Depends(get_db)):
    """Obtiene los datos de la gráfica principal."""
    logging.info("Petición recibida en /api/reporte-capacidad")
    reporte_data = db.query(ReporteCapacidadDiaria).order_by(ReporteCapacidadDiaria.fecha).all()
    return reporte_data

@app.get("/api/pedidos/{fecha_programacion}", response_model=List[PedidoSchema])
def get_pedidos_por_fecha(fecha_programacion: date, db: Session = Depends(get_db)):
    """Obtiene la lista de pedidos para una fecha específica."""
    logging.info(f"Petición recibida en /api/pedidos/{fecha_programacion}")
    pedidos_data = (
        db.query(Pedido)
        .filter(Pedido.fecha_programacion_asignada == fecha_programacion)
        .order_by(Pedido.prioridad, Pedido.fecha_ingreso)
        .all()
    )
    return pedidos_data

# --- ENDPOINT /api/pedidos/all ELIMINADO ---

