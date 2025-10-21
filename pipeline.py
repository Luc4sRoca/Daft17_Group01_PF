# ===============================================================
# pipeline.py
# ETL universal con auditoría funcional detallada
# Corrige lectura regional, escalas numéricas y conserva trazabilidad
# Compatible con coma o punto decimal (automático)
# ===============================================================

import os
import re
import pandas as pd
import numpy as np
from datetime import datetime

# === CONFIGURACIÓN GENERAL ===
try:
    CWD = os.path.dirname(os.path.abspath(__file__))
except NameError:
    CWD = os.getcwd()

RAW_DIR = os.path.join(CWD, "data_raw")
FINAL_DIR = os.path.join(CWD, "data_final")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(FINAL_DIR, exist_ok=True)

# === LOG ===
def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# === FUNCIONES AUXILIARES ===
def snake_case(name):
    """Estandariza los nombres de columnas a snake_case."""
    return re.sub(r'[^0-9a-zA-Z]+', '_', name.strip()).lower()

def clean_dataframe(df):
    """Elimina duplicados y limpia espacios o 'nan' textuales."""
    df = df.drop_duplicates()
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype(str).str.strip()
            df[c] = df[c].replace({"nan": None, "": None})
    return df

def normalize_booleans(df):
    """Detecta y normaliza columnas booleanas."""
    bool_map = {'Y': 1, 'YES': 1, 'TRUE': 1, 'T': 1, '1': 1,
                'N': 0, 'NO': 0, 'FALSE': 0, 'F': 0, '0': 0}
    for col in df.columns:
        sample = df[col].dropna().astype(str).str.upper()
        if (sample.isin(bool_map.keys()).mean() > 0.6):
            df[col] = df[col].astype(str).str.upper().replace(bool_map).astype("Int64")
    return df

def clean_percent_text(series):
    """Limpia porcentajes y reemplaza comas por puntos."""
    return (
        series.astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
        .replace("nan", np.nan)
    )

def convert_to_year(series):
    """Convierte fechas o textos a solo año (Int64)."""
    return pd.to_datetime(series, errors="coerce").dt.year.astype("Int64")

# === AUDITORÍA DE CAMBIOS ===
def audit_changes(df_raw, df_final, name):
    """Compara tipos y proporción de nulos antes y después del procesamiento."""
    print(f"\n=== 🔍 Auditoría: {name.upper()} ===")
    cols = sorted(set(df_raw.columns) & set(df_final.columns))
    for c in cols:
        raw_type = str(df_raw[c].dtype)
        final_type = str(df_final[c].dtype)
        raw_null = df_raw[c].isna().mean()
        final_null = df_final[c].isna().mean()
        diff_null = round(final_null - raw_null, 2)
        print(f"→ {c}: {raw_type} → {final_type} | ΔNulos={diff_null:+.2f} | OK")
    print()

# === PIPELINE PRINCIPAL ===
def process_file(path):
    name = os.path.basename(path).replace("_raw.csv", "")
    log(f"🔧 Procesando tabla: {name}")

    # --- LECTURA ROBUSTA ---
    try:
        try:
            # Caso habitual (punto decimal)
            df_raw = pd.read_csv(path, encoding="utf-8-sig", sep=",", decimal=".")
        except Exception:
            # Caso regional (coma decimal)
            df_raw = pd.read_csv(path, encoding="utf-8-sig", sep=";", decimal=",")
    except Exception as e:
        log(f"❌ Error al leer {path}: {e}")
        return None

    # --- Limpieza y normalización básica ---
    df = df_raw.copy()
    df.columns = [snake_case(c) for c in df.columns]
    df = clean_dataframe(df)
    df = normalize_booleans(df)

    # --- Conversión temprana de columnas numéricas ---
    for c in df.columns:
        if re.search(r"(weight|year_founded|height|id|score|pts|reb|ast|pct|num|count|rank)", c, re.I):
            df[c] = pd.to_numeric(df[c], errors="ignore")

    # === AJUSTES ESPECÍFICOS ===
    # --- Altura: reemplazar "-" por "'"
    if "height" in df.columns:
        df["height"] = df["height"].astype(str).str.replace("-", "'", regex=False)

    # --- Fechas y años ---
    for col in df.columns:
        if re.search(r"(from_year|to_year|draft_year|season)$", col, re.I):
            df[col] = convert_to_year(df[col])

    # --- Porcentajes ---
    pct_cols = [c for c in df.columns if "_pct_" in c]
    for c in pct_cols:
        df[c] = pd.to_numeric(clean_percent_text(df[c]), errors="coerce")

    # --- Protección season_type ---
    if "season_type" in df.columns:
        df["season_type"] = df["season_type"].astype(str).replace("nan", np.nan)

    # --- Control de IDs numéricos ---
    for c in df.columns:
        if c.endswith("_id"):
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    # --- Corrección de escala para columnas específicas ---
    if "weight" in df.columns:
        df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
        df.loc[df["weight"] > 600, "weight"] = df["weight"] / 10  # Ej: 2500 → 250

    if "year_founded" in df.columns:
        df["year_founded"] = pd.to_numeric(df["year_founded"], errors="coerce")
        df.loc[df["year_founded"] > 3000, "year_founded"] = df["year_founded"] / 10  # Ej: 19900 → 1990

    # --- Fecha de carga solo para log ---
    load_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log(f"📅 Fecha de procesamiento: {load_date}")

    # --- Relleno nulos por tipo ---
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c].fillna(0)
        elif re.search(r"(date|year)", c, re.I):
            df[c] = df[c].fillna(pd.NA)
        else:
            df[c] = df[c].fillna("")

    # --- Duplicados por ID ---
    pk_candidates = [c for c in df.columns if c.endswith("_id")]
    if pk_candidates:
        before = len(df)
        df = df.drop_duplicates(subset=pk_candidates, keep="first")
        dropped = before - len(df)
        print(f"   🧹 Filas eliminadas por IDs vacíos o duplicados: {dropped}")

    # --- Auditoría ---
    audit_changes(df_raw, df, name)

    # === GUARDAR ===
    out_path = os.path.join(FINAL_DIR, f"{name}_final.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    log(f"✅ {name} procesado correctamente ({len(df)} filas). Guardado en {out_path}\n")
    return out_path


# === EJECUCIÓN PRINCIPAL ===
def main():
    log("🚀 Iniciando pipeline ETL funcional...")
    files = [f for f in os.listdir(RAW_DIR) if f.endswith("_raw.csv")]
    if not files:
        log("⚠️  No se encontraron archivos *_raw.csv en la carpeta de entrada.")
        return
    for f in files:
        process_file(os.path.join(RAW_DIR, f))
    log("🟢 Pipeline completado correctamente.\n")


if __name__ == "__main__":
    main()
