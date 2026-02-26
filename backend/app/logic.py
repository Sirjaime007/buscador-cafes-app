# app/logic.py

import time
import re
import unicodedata
from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List

import pandas as pd
from geopy.geocoders import ArcGIS
from geopy.distance import geodesic

from .settings import GID_CAFES, GID_TOSTADORES, sheet_url, CACHE_TTL

# =========================
# Cache simple en memoria
# =========================
@dataclass
class CacheItem:
    ts: float
    df: pd.DataFrame

class DataCache:
    def __init__(self):
        self.cafes_por_ciudad: Dict[str, CacheItem] = {}
        self.tostadores: Optional[CacheItem] = None
        self.todos_los_cafes: Optional[CacheItem] = None

    def is_fresh(self, item: Optional[CacheItem]) -> bool:
        return item is not None and (time.time() - item.ts) < CACHE_TTL

cache = DataCache()

# =========================
# Geocoder (recurso)
# =========================
_geocoder: Optional[ArcGIS] = None

def get_geocoder() -> ArcGIS:
    global _geocoder
    if _geocoder is None:
        _geocoder = ArcGIS(timeout=10)
    return _geocoder

# =========================
# Utilidades de texto
# =========================
def normalizar_texto(valor, fallback="Sin dato"):
    if pd.isna(valor):
        return fallback
    texto = str(valor).strip()
    if texto == "" or texto.lower() == "nan":
        return fallback
    return texto

def texto_normalizado(valor: str) -> str:
    texto = normalizar_texto(valor, fallback="")
    texto = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    texto = re.sub(r"[^a-zA-Z0-9\s]", " ", texto.lower())
    return re.sub(r"\s+", " ", texto).strip()

# =========================
# Carga de datos
# =========================
def cargar_cafes(ciudad: str) -> pd.DataFrame:
    """Carga cafés para una ciudad desde Google Sheets (con cache)."""
    if ciudad not in GID_CAFES:
        raise ValueError(f"Ciudad no válida: {ciudad}")

    cached = cache.cafes_por_ciudad.get(ciudad)
    if cache.is_fresh(cached):
        return cached.df

    gid = GID_CAFES[ciudad]
    try:
        df = pd.read_csv(sheet_url(gid), dtype=str)
    except Exception:
        # Fallback opcional a CSV local si existiera en el servidor
        df = pd.read_csv("Cafes.csv", dtype=str)

    # Limpieza de coordenadas
    df["LAT"] = pd.to_numeric(df["LAT"].str.replace(",", ".", regex=False), errors="coerce")
    df["LONG"] = pd.to_numeric(df["LONG"].str.replace(",", ".", regex=False), errors="coerce")
    df = df.dropna(subset=["LAT", "LONG"])
    df = df[(df["LAT"] >= -90) & (df["LAT"] <= 90)]
    df = df[(df["LONG"] >= -180) & (df["LONG"] <= 180)]

    # Guarda ciudad
    df["CIUDAD"] = ciudad

    cache.cafes_por_ciudad[ciudad] = CacheItem(time.time(), df)
    return df

def cargar_tostadores() -> pd.DataFrame:
    if cache.is_fresh(cache.tostadores):
        return cache.tostadores.df

    try:
        df = pd.read_csv(sheet_url(GID_TOSTADORES), dtype=str)
    except Exception:
        df = pd.DataFrame(columns=["TOSTADOR", "VARIEDADES", "DESCRIPCION", "INSTAGRAM", "CIUDAD"])

    cache.tostadores = CacheItem(time.time(), df)
    return df

def cargar_todos_los_cafes() -> pd.DataFrame:
    if cache.is_fresh(cache.todos_los_cafes):
        return cache.todos_los_cafes.df

    dfs = []
    for ciudad in GID_CAFES.keys():
        try:
            df = cargar_cafes(ciudad).copy()
            dfs.append(df)
        except Exception:
            continue

    if dfs:
        df_all = pd.concat(dfs, ignore_index=True)
    else:
        df_all = pd.DataFrame()

    cache.todos_los_cafes = CacheItem(time.time(), df_all)
    return df_all

# =========================
# Geocodificación
# =========================
def geocodificar(direccion: str, ciudad: str) -> Optional[Tuple[float, float]]:
    """Intenta geocodificar online con ArcGIS."""
    if not direccion or not direccion.strip():
        return None
    geo = get_geocoder()
    try:
        loc = geo.geocode(f"{direccion}, {ciudad}, Argentina")
    except Exception:
        return None
    if loc:
        return loc.latitude, loc.longitude
    return None

def geocodificar_desde_cafes(direccion: str, cafes_df: pd.DataFrame) -> Optional[Tuple[float, float]]:
    direccion_norm = texto_normalizado(direccion)
    if not direccion_norm:
        return None
    tokens = [t for t in direccion_norm.split() if len(t) >= 3] or direccion_norm.split()

    candidatos = cafes_df.copy()
    candidatos["UBICACION_NORM"] = candidatos["UBICACION"].fillna("").apply(texto_normalizado)
    candidatos["CAFE_NORM"] = candidatos["CAFE"].fillna("").apply(texto_normalizado)

    def score(row):
        texto = f"{row['UBICACION_NORM']} {row['CAFE_NORM']}"
        return sum(1 for tok in tokens if tok in texto)

    candidatos["MATCH"] = candidatos.apply(score, axis=1)
    mejor = candidatos.sort_values("MATCH", ascending=False).iloc[0]
    if mejor["MATCH"] <= 0:
        return None
    return float(mejor["LAT"]), float(mejor["LONG"])

def resolver_coordenadas(direccion: str, ciudad: str, cafes_df: pd.DataFrame) -> Tuple[Optional[Tuple[float,float]], Optional[str]]:
    coords = geocodificar(direccion, ciudad)
    if coords:
        return coords, "online"
    coords_local = geocodificar_desde_cafes(direccion, cafes_df)
    if coords_local:
        return coords_local, "local"
    return None, None

# =========================
# Lógica de negocio
# =========================
def cafes_en_radio(cafes_df: pd.DataFrame, coords: Tuple[float,float], radio_km: float) -> pd.DataFrame:
    cafes_calc = cafes_df.copy()
    cafes_calc["DIST_KM"] = cafes_calc.apply(
        lambda r: geodesic(coords, (r["LAT"], r["LONG"])).km,
        axis=1
    )
    return cafes_calc[cafes_calc["DIST_KM"] <= radio_km]

def distancia_corta(km: float) -> str:
    if km < 1:
        return f"{int(km*1000)} m"
    return f"{km:.2f} km"
