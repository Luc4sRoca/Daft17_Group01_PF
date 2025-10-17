# pipeline.py
# Versión ejecutable del pipeline (igual al pipeline.ipynb pero lista para watchdog)

import os, re, time
from datetime import datetime
import pandas as pd
import numpy as np

# ===== CONFIG =====
start_ts = datetime.now()
CWD = os.getcwd()
RAW_DIR = os.path.join(CWD, "data_raw")
OUT_DIR = os.path.join(CWD, "data_final")
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)
LOG_PATH = os.path.join(OUT_DIR, f"pipeline_run_{start_ts.strftime('%Y%m%d_%H%M%S')}.txt")

def log(msg):
    print(msg)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(str(msg) + "\n")

log(f"Pipeline start: {start_ts.isoformat(timespec='seconds')}")
log(f"RAW_DIR={RAW_DIR} | OUT_DIR={OUT_DIR}")

# ===== HELPERS =====
def snake_case(name: str) -> str:
    name = re.sub(r"[^\w]+", "_", name)
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    name = re.sub(r"_+", "_", name)
    return name.lower().strip("_")

def load_any(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv": return pd.read_csv(path, low_memory=False)
    if ext in [".xlsx", ".xls"]: return pd.read_excel(path)
    if ext == ".json":
        try: return pd.read_json(path, orient="records")
        except ValueError: return pd.read_json(path, lines=True)
    if ext == ".parquet": return pd.read_parquet(path)
    raise ValueError(f"Unsupported file type: {ext}")

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [snake_case(c) for c in df.columns]
    df = df.dropna(how="all")
    for c in df.columns:
        try:
            df[c] = df[c].astype(str).str.strip().replace(
                {"": np.nan, "None": np.nan, "NULL": np.nan, "nan": np.nan, "NaN": np.nan}
            )
        except Exception:
            pass
        c_low = c.lower()
        if any(k in c_low for k in ["id","game_id","player_id","person_id","team_id"]):
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
        elif any(k in c_low for k in ["date","dt","fecha"]):
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

# ===== NORMALIZADOR DE common_player_info =====
def normalize_common_player_info(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()

    # Renombres típicos de ID
    cols = {c.lower(): c for c in df.columns}
    if "person_id" in cols: df.rename(columns={cols["person_id"]: "player_id"}, inplace=True)
    if "playerid" in cols: df.rename(columns={cols["playerid"]: "player_id"}, inplace=True)
    if "teamid" in cols: df.rename(columns={cols["teamid"]: "team_id"}, inplace=True)

    # height: solo reemplaza "-" por ","
    if "height" in df.columns:
        df["height"] = df["height"].astype(str).str.replace("-", ",", regex=False)

    # weight: si pesa más de 500, dividir por 10
    if "weight" in df.columns:
        df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
        df["weight"] = df["weight"].apply(lambda x: x / 10 if pd.notna(x) and x > 500 else x)

    # season_exp: numérico, mantiene ceros
    if "season_exp" in df.columns:
        df["season_exp"] = df["season_exp"].replace({"R": "0"})
        df["season_exp"] = pd.to_numeric(df["season_exp"], errors="coerce").fillna(0).astype("Int64")

    return df

# ===== VALIDACIONES =====
def validate_pk(df, cols):
    if not all(c in df.columns for c in cols):
        return False, f"Missing columns: {set(cols)-set(df.columns)}"
    dups = df.duplicated(subset=cols).sum()
    return dups == 0, f"Duplicate rows: {dups}"

def validate_fk(df_from, col_from, df_to, col_to):
    if col_from not in df_from.columns or col_to not in df_to.columns:
        return False, f"Columns not found: {col_from} or {col_to}"
    missing = ~df_from[col_from].isin(df_to[col_to])
    return missing.sum() == 0, f"Unmatched rows: {missing.sum()}"

# ===== STAGE 1: LOAD + CLEAN =====
t0 = time.time()
log("Stage 1: Load and clean all *_raw files")
raw_files = [f for f in os.listdir(RAW_DIR)
             if f.endswith(("_raw.csv","_raw.xlsx","_raw.xls","_raw.json","_raw.parquet"))]
dfs = {}
if not raw_files:
    log("No *_raw files found in data_raw/")
else:
    for fname in raw_files:
        try:
            path = os.path.join(RAW_DIR, fname)
            df = load_any(path)
            df = clean_dataframe(df)
            key = fname.replace("_raw", "").split(".")[0]
            dfs[key] = df
            log(f"Loaded and cleaned {fname} ({df.shape[0]} rows, {df.shape[1]} cols)")
        except Exception as e:
            log(f"Error reading {fname}: {e}")

# ===== NORMALIZAR IDs =====
log("Normalizing ID columns")
for k, df in dfs.items():
    rename_map = {}
    for c in df.columns:
        lc = c.lower()
        if lc in ["person_id", "playerid", "player_id"]: rename_map[c] = "player_id"
        elif lc in ["teamid", "team_id"] or (lc == "id" and "team" in k): rename_map[c] = "team_id"
        elif lc in ["gameid", "game_id"]: rename_map[c] = "game_id"
        elif lc == "id" and "player" in k: rename_map[c] = "player_id"
    if rename_map:
        df.rename(columns=rename_map, inplace=True)
        log(f"{k}: renamed {rename_map}")
    dfs[k] = df

# ===== NORMALIZAR common_player_info =====
if "common_player_info" in dfs:
    dfs["common_player_info"] = normalize_common_player_info(dfs["common_player_info"])
    log("Normalized common_player_info (height, weight, season_exp)")

# ===== VALIDACIONES =====
t1 = time.time()
log("Stage 2: PK/FK validation")

player = dfs.get("player", pd.DataFrame())
team   = dfs.get("team", pd.DataFrame())
game   = dfs.get("game", pd.DataFrame())
summary= dfs.get("game_summary", pd.DataFrame())
stats  = dfs.get("other_stats", pd.DataFrame())
cpi    = dfs.get("common_player_info", pd.DataFrame())

if not team.empty and "team_id" in team.columns:
    ok, msg = validate_pk(team, ["team_id"]); log(f"PK team_id: {ok}. {msg}")
if not player.empty and "player_id" in player.columns:
    ok, msg = validate_pk(player, ["player_id"]); log(f"PK player_id: {ok}. {msg}")
if not game.empty and "game_id" in game.columns:
    ok, msg = validate_pk(game, ["game_id"]); log(f"PK game_id: {ok}. {msg}")
if not cpi.empty and "player_id" in cpi.columns:
    ok, msg = validate_pk(cpi, ["player_id"]); log(f"PK common_player_info.player_id: {ok}. {msg}")

# ===== EXPORT =====
t2 = time.time()
log("Stage 3: Export final tables")

for key, df in dfs.items():
    out_name = f"{key}_final.csv"
    out_path = os.path.join(OUT_DIR, out_name)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    log(f"Saved {out_name} ({df.shape[0]} rows, {df.shape[1]} cols)")

# ===== SNAPSHOT =====
status_file = os.path.join(OUT_DIR, "data_status.csv")
current = {k: (v.shape[0] if isinstance(v, pd.DataFrame) and not v.empty else 0) for k, v in dfs.items()}
prev = {}
if os.path.exists(status_file):
    try:
        prev_df = pd.read_csv(status_file)
        prev = dict(zip(prev_df["table"], prev_df["rows"]))
    except Exception:
        prev = {}
changes = {k: n - prev.get(k, 0) for k, n in current.items() if n - prev.get(k, 0) != 0}
if changes:
    log("Changes since last run:")
    for k, d in changes.items():
        sign = "+" if d >= 0 else ""
        log(f" - {k}: {sign}{d} rows")
else:
    log("No ingestion changes since last run.")
pd.DataFrame({"table": list(current.keys()), "rows": list(current.values())}).to_csv(status_file, index=False, encoding="utf-8-sig")
log(f"Saved ingestion snapshot to data_final/data_status.csv")

# ===== END =====
end_ts = datetime.now()
log(f"Pipeline completed in {(end_ts - start_ts).total_seconds():.2f}s")
