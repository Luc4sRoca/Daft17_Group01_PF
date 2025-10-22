# ===============================================================
# watchdog_ingestion.py — Versión estable y segura (vFinal)
# Monitorea data_raw/, ejecuta pipeline.py y sube los *_final.csv a SQL Server.
# ===============================================================

import time
import os
from datetime import datetime
import subprocess
import pandas as pd
import pyodbc
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# === CONFIGURACIÓN GENERAL ===
CWD = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(CWD, "data_raw")
FINAL_DIR = os.path.join(CWD, "data_final")
PIPELINE_SCRIPT = os.path.join(CWD, "pipeline.py")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(FINAL_DIR, exist_ok=True)

# === CONFIGURACIÓN SQL SERVER ===
SQL_CONFIG = {
    "DRIVER": "ODBC Driver 17 for SQL Server",
    "SERVER": "PCLUCAS\\SQLEXPRESS",
    "DATABASE": "NBA_Project",
    "USER": "sa",
    "PASSWORD": "1234"
}

# === CONEXIÓN SQL ===
def connect_sql():
    conn_str = (
        f"DRIVER={{{SQL_CONFIG['DRIVER']}}};"
        f"SERVER={SQL_CONFIG['SERVER']};"
        f"DATABASE={SQL_CONFIG['DATABASE']};"
        f"UID={SQL_CONFIG['USER']};"
        f"PWD={SQL_CONFIG['PASSWORD']}"
    )
    return pyodbc.connect(conn_str, timeout=5)

def test_sql_connection():
    try:
        with connect_sql():
            print("✅ Conexión con SQL Server verificada correctamente.\n")
    except Exception as e:
        print(f"❌ No se pudo conectar con SQL Server: {e}\n")
        raise

# === CREAR TABLA AUTOMÁTICAMENTE SI NO EXISTE ===
def ensure_table_exists(cursor, table_name, df):
    pk_candidates = ["id", "person_id", "game_id"]
    columns_def = []

    for col, dtype in df.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            sql_type = "INT"
        elif pd.api.types.is_float_dtype(dtype):
            sql_type = "FLOAT"
        else:
            sql_type = "NVARCHAR(255)"
        not_null = "NOT NULL" if col in pk_candidates else "NULL"
        columns_def.append(f"[{col}] {sql_type} {not_null}")

    cols_sql = ", ".join(columns_def)
    cursor.execute(
        f"""
        IF OBJECT_ID('{table_name}', 'U') IS NULL
        CREATE TABLE {table_name} ({cols_sql});
        """
    )

# === LIMPIEZA UNIVERSAL DE DATOS ===
def clean_invalid_floats(df):
    """Limpia valores no válidos antes de subir a SQL."""
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = (
                df[col]
                .astype(str)
                .replace({
                    "nan": None, "NaN": None, "None": None,
                    "inf": None, "-inf": None, "": None, " ": None,
                })
            )
    return df

# === SUBIDA A SQL SERVER ===
def upload_csv_to_sql(file_path):
    try:
        df = pd.read_csv(file_path)
        df = clean_invalid_floats(df)
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        print(f"🗄️  Subiendo tabla: {table_name} ...")

        conn = connect_sql()
        cursor = conn.cursor()

        ensure_table_exists(cursor, table_name, df)
        cursor.execute(f"DELETE FROM {table_name};")
        conn.commit()

        cols = ','.join([f"[{c}]" for c in df.columns])
        total_rows = len(df)
        inserted = 0

        for row in df.itertuples(index=False, name=None):
            placeholders = ','.join(['?' for _ in row])
            try:
                cursor.execute(f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders});", row)
                inserted += 1
            except Exception as e:
                print(f"⚠️  Fila omitida en {table_name}: {e}")
                continue

        conn.commit()
        cursor.close()
        conn.close()

        print(f"✅ {table_name} cargado correctamente ({inserted}/{total_rows} filas).")

        # Mini vista previa
        try:
            conn = connect_sql()
            preview = pd.read_sql_query(f"SELECT TOP 3 * FROM {table_name}", conn)
            print(f"\n📋 Vista previa de {table_name}:")
            print(preview)
            conn.close()
        except Exception as e:
            print(f"⚠️ No se pudo mostrar vista previa de {table_name}: {e}")

    except Exception as e:
        print(f"❌ Error subiendo {file_path}: {e}")

# === WATCHDOG CON CONTROL DE REPETICIÓN ===
class PipelineTrigger(FileSystemEventHandler):
    last_run_time = 0
    cooldown_seconds = 15  # evita repeticiones seguidas

    def on_any_event(self, event):
        if event.is_directory:
            return
        if not event.src_path.endswith(("_raw.csv", "_raw.xlsx", "_raw.xls", "_raw.json", "_raw.parquet")):
            return

        now = time.time()
        if now - self.last_run_time < self.cooldown_seconds:
            print(f"⏸️  Evento ignorado (cooldown de {self.cooldown_seconds}s)")
            return

        self.last_run_time = now
        fname = os.path.basename(event.src_path)
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🔄 Cambio detectado en: {fname}")
        self.run_pipeline_and_upload()

    def run_pipeline_and_upload(self):
        print(f"🚀 Ejecutando pipeline: {PIPELINE_SCRIPT}")
        try:
            subprocess.run(["python", PIPELINE_SCRIPT], check=True)
            print("✅ Pipeline ejecutado correctamente.")
        except subprocess.CalledProcessError as e:
            print(f"❌ Error ejecutando pipeline: {e}")
            return

        print(f"\n📂 Buscando archivos *_final.csv en {FINAL_DIR} ...")
        try:
            files = [f for f in os.listdir(FINAL_DIR) if f.endswith("_final.csv")]
            if not files:
                print("⚠️  No se encontraron archivos *_final.csv.")
                return
            for file in files:
                file_path = os.path.join(FINAL_DIR, file)
                upload_csv_to_sql(file_path)
            print("\n🟢 Carga completa en SQL Server finalizada.\n")
        except Exception as e:
            print(f"❌ Error durante la carga: {e}")

# === MAIN ===
if __name__ == "__main__":
    try:
        print("=" * 60)
        print(f"👁️  Watchdog activo en: {RAW_DIR}")
        print(f"📦  Pipeline ejecutable: {PIPELINE_SCRIPT}")
        print(f"🎯  Subirá resultados desde: {FINAL_DIR}")
        print("=" * 60)

        test_sql_connection()
        print("Esperando nuevos archivos *_raw.* o modificaciones...\n")

        event_handler = PipelineTrigger()
        observer = Observer()
        observer.schedule(event_handler, RAW_DIR, recursive=False)
        observer.start()

        while True:
            time.sleep(2)

    except Exception as e:
        print(f"\n❌ Error crítico en watchdog: {e}")

    finally:
        print("\n🛑 Watchdog detenido.")
        input("Presioná ENTER para cerrar...")
