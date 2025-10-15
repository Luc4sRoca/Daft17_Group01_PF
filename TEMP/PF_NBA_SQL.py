#!/usr/bin/env python
# coding: utf-8

# ==========================================================
# SCRIPT ETL AUTOMATIZADO – Proyecto NBA
# Autor: Lucas Roca
# ==========================================================

# --------------------- IMPORTS ----------------------------
import os
import re
import pandas as pd
from datetime import datetime
import numpy as np  
from pandas.api.types import is_integer_dtype
# ----------------------------------------------------------

# ==========================================================
# Función utilitaria: normalización de año (corrige 1996.0 → 1996)
# ==========================================================
def year_first4(x):
    """Devuelve los primeros 4 dígitos encontrados (como int) o NA."""
    if pd.isna(x):
        return pd.NA
    s = str(x).strip()
    m = re.search(r'\d{4}', s)
    return int(m.group(0)) if m else pd.NA

# ==========================================================
# AJUSTE DE RUTA BASE Y TIEMPO DE EJECUCIÓN
# ==========================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)
print(f"📂 Directorio base establecido en: {BASE_DIR}")

inicio = datetime.now()
print("🟢 Inicio del proceso ETL:", inicio.strftime("%Y-%m-%d %H:%M:%S"))

# ==========================================================
# BLOQUE 1: CARGA Y ENRIQUECIMIENTO PLAYER
# ==========================================================
try:
    player = pd.read_csv("data/raw/player.csv")
    common = pd.read_csv("data/raw/common_player_info.csv")

    # Normalizar columnas de año al cargar
    for col in ["from_year", "to_year"]:
        if col in player.columns:
            player[col] = player[col].apply(year_first4).astype("Int64")
        if col in common.columns:
            common[col] = common[col].apply(year_first4).astype("Int64")

    print("✅ player y common cargados y normalizados correctamente")
except Exception as e:
    print(f"❌ Error cargando player/common: {e}")

# Unir tablas
player_enriched = player.merge(common, how='left', left_on='id', right_on='person_id')

# Reforzar tipo de datos de año después del merge
for col in ["from_year", "to_year"]:
    if col in player_enriched.columns:
        player_enriched[col] = player_enriched[col].apply(year_first4).astype("Int64")

# Eliminar columnas redundantes
cols_drop = [
    'first_name_y','last_name_y','display_first_last','display_last_comma_first',
    'display_fi_last','player_slug','last_affiliation','team_code',
    'team_abbreviation','playercode','games_played_current_season_flag','first_name_x', 
    'last_name_x', 'person_id'
]
player_enriched.drop(columns=cols_drop, inplace=True, errors='ignore')

# Crear columnas nuevas
player_enriched['birthdate'] = pd.to_datetime(player_enriched['birthdate'], errors='coerce')
player_enriched['edad'] = (datetime.now() - player_enriched['birthdate']).dt.days // 365
player_enriched['años_jugando'] = player_enriched['to_year'] - player_enriched['from_year']
player_enriched['undrafted'] = player_enriched['draft_year'].isna().astype(int)

# Guardar resultado
out_path = "data/clean/player_enriched.csv"
player_enriched.columns = player_enriched.columns.str.lower()
player_enriched.to_csv(out_path, index=False)

if os.path.exists(out_path):
    print(f"✅ player_enriched creado con {len(player_enriched)} filas (años normalizados)")
else:
    print("⚠️ Error al exportar player_enriched")

# ==========================================================
# BLOQUE 2: TABLAS AUXILIARES
# ==========================================================
def limpiar_y_guardar(nombre, drop_cols=None):
    try:
        df = pd.read_csv(f"data/raw/{nombre}.csv")

        # Normalizar year_founded si existe
        if "year_founded" in df.columns:
            df["year_founded"] = df["year_founded"].apply(year_first4).astype("Int64")

        if drop_cols:
            df.drop(columns=drop_cols, inplace=True, errors='ignore')

        df.columns = df.columns.str.lower()
        df.to_csv(f"data/clean/{nombre}.csv", index=False)
        print(f"✅ {nombre}.csv limpio ({len(df)} filas)")
    except Exception as e:
        print(f"❌ Error limpiando {nombre}: {e}")

limpiar_y_guardar("team", ['abbreviation'])
limpiar_y_guardar("other_stats", ['league_id', 'team_abbreviation_home'])
limpiar_y_guardar("game_summary", ['wh_status'])
print("✅ Tablas auxiliares limpiadas correctamente")

# ==========================================================
# BLOQUE 3: FILTRAR JUGADORES NBA
# ==========================================================
try:
    df = pd.read_csv("data/clean/player_enriched.csv")
    df_nba = df[df['nba_flag'] == 1]
    df_nba.to_csv("data/clean/player_enrichedF.csv", index=False)
    print(f"✅ Jugadores NBA filtrados ({len(df_nba)} filas)")
except Exception as e:
    print(f"❌ Error filtrando jugadores NBA: {e}")

# ==========================================================
# BLOQUE 4: UNIÓN GAME + TEAM + OTHER_STATS
# ==========================================================
try:
    game = pd.read_csv("data/raw/game.csv")
    team_clean = pd.read_csv("data/clean/team.csv")

    if "year_founded" in team_clean.columns:
        team_clean["year_founded"] = team_clean["year_founded"].apply(year_first4).astype("Int64")

    print("✅ game y team cargados correctamente")

    game_team = game.merge(team_clean, how='left', left_on='team_id_home', right_on='id')
    game_team.columns = game_team.columns.str.lower()

    if "year_founded" in game_team.columns:
        game_team["year_founded"] = game_team["year_founded"].apply(year_first4).astype("Int64")

    game_team.to_csv("data/clean/game_team.csv", index=False)
    print(f"✅ game_team creado ({len(game_team)} filas)")
except Exception as e:
    print(f"❌ Error creando game_team: {e}")

try:
    other_stats = pd.read_csv("data/clean/other_stats.csv")
    game_team_other_stats = game_team.merge(other_stats, how='left', on=['game_id','game_id'])
    game_team_other_stats.columns = game_team_other_stats.columns.str.lower()
    game_team_other_stats.to_csv("data/clean/game_team_other_stats.csv", index=False)
    print(f"✅ game_team_other_stats creado ({len(game_team_other_stats)} filas)")
except Exception as e:
    print(f"❌ Error creando game_team_other_stats: {e}")

# ==========================================================
# BLOQUE 5: LIMPIEZA DE NULOS
# ==========================================================
def limpiar_nulos(df):
    df = df.loc[:, df.isna().mean() < 0.8]
    id_cols = [c for c in df.columns if 'id' in c]
    if id_cols:
        df = df.dropna(subset=id_cols)

    num_cols = df.select_dtypes(include='number').columns
    for col in num_cols:
        if df[col].isna().mean() > 0:
            if df[col].isna().mean() < 0.3:
                df[col] = df[col].fillna(df[col].median())
            else:
                df[col] = df[col].fillna(0)

    cat_cols = df.select_dtypes(include='object').columns
    for col in cat_cols:
        df[col] = df[col].fillna('Desconocido')

    # Reimponer formato de año en columnas clave
    for col in ["from_year", "to_year", "year_founded"]:
        if col in df.columns:
            df[col] = df[col].apply(year_first4).astype("Int64")

    return df

path = "data/clean/"
files = [
    "game_team.csv","game_summary.csv","other_stats.csv",
    "player_enriched.csv","team.csv","game_team_other_stats.csv","game.csv"
]

for f in files:
    try:
        file_path = os.path.join(path, f)
        df = pd.read_csv(file_path, low_memory=False)
        df = limpiar_nulos(df)
        out_path = os.path.join("data/final", f.replace('.csv', '_clean.csv'))
        df.to_csv(out_path, index=False)
        print(f"✅ {f} limpiado → {out_path} ({len(df)} filas)")
    except Exception as e:
        print(f"❌ Error procesando {f}: {e}")

# ==========================================================
# BLOQUE 6: ESTADÍSTICAS DESCRIPTIVAS
# ==========================================================
# def mostrar_estadisticas(df, tabla):
#     print(f"\n{'='*70}")
#     print(f"📊 ESTADÍSTICAS DESCRIPTIVAS – {tabla.upper()}")
#     print(f"{'='*70}")
#     num_cols = df.select_dtypes(include='number').columns
#     if len(num_cols) == 0:
#         print("⚠️ No hay columnas numéricas en esta tabla.")
#         return
#     for col in num_cols:
#         print(f"\n➡️ Columna: {col}")
#         print(df[col].describe())
#     print(f"\n{'-'*50}\nPrimeras 5 columnas numéricas (while loop):")
#     i = 0
#     while i < min(5, len(num_cols)):
#         col = num_cols[i]
#         print(f"\n🔹 Columna (while): {col}")
#         print(df[col].describe())
#         i += 1

# path = "data/final/"
# files = [
#     "game_clean.csv","game_team_clean.csv","game_summary_clean.csv",
#     "other_stats_clean.csv","player_enriched_clean.csv",
#     "team_clean.csv","game_team_other_stats_clean.csv"
# ]
# for f in files:
#     try:
#         df = pd.read_csv(os.path.join(path, f))
#         mostrar_estadisticas(df, f)
#     except Exception as e:
#         print(f"⚠️ Error mostrando estadísticas de {f}: {e}")




# ==========================================================
# RE-CÁLCULO SEGURO (al final) DE 'edad' y 'años_jugando'
# ==========================================================

# ==========================================================
# RE-CÁLCULO SEGURO (al final) DE 'edad', 'años_jugando' y 'weight'
# ==========================================================
from datetime import datetime
import numpy as np
import pandas as pd
import os

final_pl_path = os.path.join("data/final", "player_enriched_clean.csv")
if os.path.exists(final_pl_path):
    plf = pd.read_csv(final_pl_path, low_memory=False)

    # --- Recalcular edad ---
    if "birthdate" in plf.columns:
        plf["birthdate"] = (
            plf["birthdate"].astype(str)
            .str.replace(r"^(\d{4})\d+", r"\1", regex=True)
            .str.replace(r"^(\d{4})0([-/])", r"\1\2", regex=True)
        )
        plf["birthdate"] = pd.to_datetime(plf["birthdate"], errors="coerce")
        plf["edad"] = ((pd.Timestamp("today").normalize() - plf["birthdate"]).dt.days // 365).astype("Int64")

    # --- Recalcular años jugando ---
    if {"from_year", "to_year"}.issubset(plf.columns):
        plf["from_year"] = plf["from_year"].apply(year_first4).astype("Int64")
        plf["to_year"] = plf["to_year"].apply(year_first4).astype("Int64")
        plf["años_jugando"] = (plf["to_year"] - plf["from_year"]).astype("Int64")

    # --- Corrección de valores inflados (edad y años_jugando con 0 de más) ---
    if "edad" in plf.columns:
        plf["edad"] = plf["edad"].astype("Int64")
        mask_edad_bug = plf["edad"].notna() & (plf["edad"] >= 150) & ((plf["edad"] % 10) == 0)
        plf.loc[mask_edad_bug, "edad"] = (plf.loc[mask_edad_bug, "edad"] // 10).astype("Int64")

    if "años_jugando" in plf.columns:
        plf["años_jugando"] = plf["años_jugando"].astype("Int64")
        mask_aj_bug = plf["años_jugando"].notna() & (plf["años_jugando"] >= 200) & ((plf["años_jugando"] % 10) == 0)
        plf.loc[mask_aj_bug, "años_jugando"] = (plf.loc[mask_aj_bug, "años_jugando"] // 10).astype("Int64")

    # --- Normalización universal del peso ---
    if "weight" in plf.columns:
        plf["weight"] = pd.to_numeric(plf["weight"], errors="coerce")
        plf["weight"] = (plf["weight"] +0).round(0).astype("Int64")
    # --- Guardar resultado corregido ---
    plf.to_csv(final_pl_path, index=False)
    print("🔧 Reforzado: 'edad', 'años_jugando' y 'weight' recalculados/corregidos en player_enriched_clean.csv (final)")


# ==========================================================
# BLOQUE 7: CIERRE Y DURACIÓN
# ==========================================================
fin = datetime.now()
duracion = fin - inicio
print(f"\n🏁 Proceso completado: {fin.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"⏱️ Duración total: {duracion}")
print("----------------------------------------------------------")