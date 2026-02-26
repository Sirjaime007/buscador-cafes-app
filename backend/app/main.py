# app/main.py

from typing import List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .settings import GID_CAFES
from .logic import (
    cargar_cafes, cargar_tostadores, cargar_todos_los_cafes,
    resolver_coordenadas, cafes_en_radio, distancia_corta, normalizar_texto
)

app = FastAPI(title="Buscador de Cafés API", version="1.0.0")

# CORS abierto en desarrollo. En producción, restringí allow_origins.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# Modelos Pydantic
# =========================
class CafeOut(BaseModel):
    CAFE: str
    UBICACION: str
    TOSTADOR: str
    LAT: float
    LONG: float
    CIUDAD: str
    DIST_KM: Optional[float] = None
    DISTANCIA: Optional[str] = None
    MAPS: Optional[str] = None

class TostadorOut(BaseModel):
    TOSTADOR: str
    VARIEDADES: Optional[str] = "-"
    DESCRIPCION: Optional[str] = "-"
    INSTAGRAM: Optional[str] = "-"
    CIUDAD: Optional[str] = "-"

class BuscarCafesIn(BaseModel):
    ciudad: str = Field(..., description="Ej: 'Mar del Plata'")
    direccion: str = Field(..., description="Ej: 'Av. Colón 1500'")
    radio_km: float = Field(ge=0.1, le=10.0, default=2.0)
    tostador: Optional[str] = Field(default=None, description="Filtrar por tostador exacto")

class RecomendarIn(BaseModel):
    ciudad: str
    direccion: str

class RecomendarOut(BaseModel):
    cafe: CafeOut
    fuente_geocoding: str

# =========================
# Endpoints
# =========================
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/ciudades", response_model=List[str])
def ciudades():
    return list(GID_CAFES.keys())

@app.get("/cafes", response_model=List[CafeOut])
def get_cafes(ciudad: str):
    if ciudad not in GID_CAFES:
        raise HTTPException(status_code=400, detail="Ciudad inválida")
    df = cargar_cafes(ciudad)
    out: List[CafeOut] = []
    for _, r in df.iterrows():
        out.append(CafeOut(
            CAFE=normalizar_texto(r.get("CAFE")),
            UBICACION=normalizar_texto(r.get("UBICACION")),
            TOSTADOR=normalizar_texto(r.get("TOSTADOR"), fallback="Sin tostador cargado"),
            LAT=float(r["LAT"]),
            LONG=float(r["LONG"]),
            CIUDAD=str(r.get("CIUDAD", ciudad)),
        ))
    return out

@app.get("/tostadores", response_model=List[TostadorOut])
def get_tostadores(ciudad: Optional[str] = None):
    df = cargar_tostadores().fillna("-")
    if ciudad:
        df = df[df["CIUDAD"].str.contains(ciudad, case=False, na=False)]
    out: List[TostadorOut] = []
    for _, r in df.iterrows():
        out.append(TostadorOut(
            TOSTADOR=str(r["TOSTADOR"]),
            VARIEDADES=str(r["VARIEDADES"]),
            DESCRIPCION=str(r["DESCRIPCION"]),
            INSTAGRAM=str(r["INSTAGRAM"]),
            CIUDAD=str(r["CIUDAD"])
        ))
    return out

@app.post("/buscar-cafes", response_model=List[CafeOut])
def buscar_cafes(payload: BuscarCafesIn):
    if payload.ciudad not in GID_CAFES:
        raise HTTPException(status_code=400, detail="Ciudad inválida")

    cafes_df = cargar_cafes(payload.ciudad)
    coords, _ = resolver_coordenadas(payload.direccion, payload.ciudad, cafes_df)
    if not coords:
        raise HTTPException(status_code=404, detail="No se pudo geocodificar la dirección")

    resultado = cafes_en_radio(cafes_df, coords, payload.radio_km).copy().sort_values("DIST_KM")

    if payload.tostador and payload.tostador != "Todos":
        resultado = resultado[resultado["TOSTADOR"] == payload.tostador]

    if resultado.empty:
        return []

    resultado["DISTANCIA"] = resultado["DIST_KM"].apply(distancia_corta)
    resultado["MAPS"] = resultado.apply(
        lambda r: f"https://www.google.com/maps/search/?api=1&query={r['LAT']},{r['LONG']}",
        axis=1
    )

    out: List[CafeOut] = []
    for _, r in resultado.iterrows():
        out.append(CafeOut(
            CAFE=normalizar_texto(r.get("CAFE")),
            UBICACION=normalizar_texto(r.get("UBICACION")),
            TOSTADOR=normalizar_texto(r.get("TOSTADOR"), fallback="Sin tostador cargado"),
            LAT=float(r["LAT"]),
            LONG=float(r["LONG"]),
            CIUDAD=str(r.get("CIUDAD", payload.ciudad)),
            DIST_KM=float(r["DIST_KM"]),
            DISTANCIA=str(r["DISTANCIA"]),
            MAPS=str(r["MAPS"])
        ))
    return out

@app.post("/recomendar-cafe", response_model=RecomendarOut)
def recomendar_cafe(payload: RecomendarIn):
    if payload.ciudad not in GID_CAFES:
        raise HTTPException(status_code=400, detail="Ciudad inválida")

    cafes_df = cargar_cafes(payload.ciudad)
    coords, fuente = resolver_coordenadas(payload.direccion, payload.ciudad, cafes_df)
    if not coords:
        raise HTTPException(status_code=404, detail="No se pudo geocodificar la dirección")

    recomendados = cafes_en_radio(cafes_df, coords, 0.75)
    if recomendados.empty:
        raise HTTPException(status_code=404, detail="No hay cafés en 750 m para recomendar")

    recomendado = recomendados.sample(n=1).iloc[0]
    cafe = CafeOut(
        CAFE=normalizar_texto(recomendado.get("CAFE")),
        UBICACION=normalizar_texto(recomendado.get("UBICACION")),
        TOSTADOR=normalizar_texto(recomendado.get("TOSTADOR"), fallback="Sin tostador cargado"),
        LAT=float(recomendado["LAT"]),
        LONG=float(recomendado["LONG"]),
        CIUDAD=payload.ciudad,
        DIST_KM=float(recomendado["DIST_KM"]),
        DISTANCIA=distancia_corta(float(recomendado["DIST_KM"])),
        MAPS=f"https://www.google.com/maps/search/?api=1&query={recomendado['LAT']},{recomendado['LONG']}"
    )
    return RecomendarOut(cafe=cafe, fuente_geocoding=fuente or "desconocida")

@app.get("/buscar-por-nombre", response_model=List[CafeOut])
def buscar_por_nombre(nombre: str):
    df = cargar_todos_los_cafes()
    if df.empty:
        return []
    resultados = df[df["CAFE"] == nombre].copy()
    if resultados.empty:
        return []
    resultados["MAPS"] = resultados.apply(
        lambda r: f"https://www.google.com/maps/search/?api=1&query={r['LAT']},{r['LONG']}",
        axis=1
    )
    out: List[CafeOut] = []
    for _, r in resultados.iterrows():
        out.append(CafeOut(
            CAFE=normalizar_texto(r.get("CAFE")),
            UBICACION=normalizar_texto(r.get("UBICACION")),
            TOSTADOR=normalizar_texto(r.get("TOSTADOR"), fallback="Sin tostador cargado"),
            LAT=float(r["LAT"]),
            LONG=float(r["LONG"]),
            CIUDAD=str(r.get("CIUDAD", "")),
            MAPS=str(r["MAPS"])
        ))
    return out
