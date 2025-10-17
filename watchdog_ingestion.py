# ===============================================================
# watchdog_ingestion.py
# Versión profesional automatizada: detecta archivos *_raw.*
# Ejecuta el pipeline y sube los resultados *_final.csv a SQL Server.
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
CWD = os.getcwd()
RAW_DIR = os.path.join(CWD, "data_raw")
FINAL_DIR = os.path.join(CWD, "data", "final")
PIPELINE_SCRIPT = os.path.join(CWD, "pipeline.py")

# === CONEXIÓN SQL SERVER ===
SQL_CONFIG = {
    "DRIVER": "ODBC Driver 17 for SQL Server",
    "SERVER": "localhost\\SQLEXPRESS",   # o tu instancia real
    "DATABASE": "NBA_Project",
    "USER": "sa",
    "PASSWORD": "tu_contraseña_segura"
}

# === FUNCIÓN PARA CARGAR CSV A SQL SERVER ===
def upload_csv_to_sql(file_path):
    try:
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        print(f"🗄️  Subiendo {table_name} a SQL Server...")

        conn_str = (
            f"DRIVER={{{SQL_CONFIG['DRIVER']}}};"
            f"SERVER={SQL_CONFIG['SERVER']};"
            f"DATABASE={SQL_CONFIG['DATABASE']};"
            f"UID={SQL_CONFIG['USER']};"
            f"PWD={SQL_CONFIG['PASSWORD']}"
        )

        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        df = pd.read_csv(file_path)
        cols = ','.join([f"[{c}]" for c in df.columns])

        # Elimina datos previos antes de cargar nuevos (modo truncado)
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL TRUNCATE TABLE {table_name};")
        conn.commit()

        # Inserta fila por fila (puede optimizarse con bulk insert)
        for row in df.itertuples(index=False, name=None):
            placeholders = ','.join(['?' for _ in row])
            cursor.execute(f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders});", row)

        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ {table_name} cargado correctamente ({len(df)} filas).")

    except Exception as e:
        print(f"❌ Error subiendo {file_path}: {e}")

# === CLASE WATCHDOG ===
class PipelineTrigger(FileSystemEventHandler):
    def on_any_event(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith(("_raw.csv", "_raw.xlsx", "_raw.xls", "_raw.json", "_raw.parquet")):
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

        # === Buscar archivos *_final.csv para subirlos ===
        print("📂 Buscando archivos *_final.csv en carpeta FINAL...")
        try:
            for file in os.listdir(FINAL_DIR):
                if file.endswith("_final.csv"):
                    file_path = os.path.join(FINAL_DIR, file)
                    upload_csv_to_sql(file_path)
            print("🟢 Carga completa de archivos FINAL finalizada.\n")
        except Exception as e:
            print(f"❌ Error durante la carga a SQL: {e}")

# === MAIN ===
if __name__ == "__main__":
    print("="*60)
    print(f"👁️  Watchdog activo en: {RAW_DIR}")
    print(f"📦  Pipeline ejecutable: {PIPELINE_SCRIPT}")
    print(f"🎯  Subirá resultados desde: {FINAL_DIR}")
    print("="*60)
    print("Esperando nuevos archivos *_raw.* o modificaciones...\n")

    event_handler = PipelineTrigger()
    observer = Observer()
    observer.schedule(event_handler, RAW_DIR, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(2)
    except KeyboardInterrupt:
        print("\n🛑 Watchdog detenido manualmente.")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    try:
        print("="*60)
        print(f"👁️  Watchdog activo en: {RAW_DIR}")
        print(f"📦  Pipeline ejecutable: {PIPELINE_SCRIPT}")
        print(f"🎯  Subirá resultados desde: {FINAL_DIR}")
        print("="*60)
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
        print("\n🛑 Watchdog detenido (fin de ejecución).")
        input("Presioná ENTER para cerrar...")
