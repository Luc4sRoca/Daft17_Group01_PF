# watchdog_ingestion.py
# Versión profesional controlada para defensa: automatiza la ejecución del pipeline al detectar nuevos archivos *_raw.*

import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import subprocess
from datetime import datetime

# === CONFIGURACIÓN ===
CWD = os.getcwd()
RAW_DIR = os.path.join(CWD, "data_raw")
PIPELINE_SCRIPT = os.path.join(CWD, "pipeline.py")  # Pipeline ejecutable

class PipelineTrigger(FileSystemEventHandler):
    def on_any_event(self, event):
        if event.is_directory:
            return
        # Detecta creación o modificación de archivos *_raw
        if event.src_path.endswith(("_raw.csv", "_raw.xlsx", "_raw.xls", "_raw.json", "_raw.parquet")):
            fname = os.path.basename(event.src_path)
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🔄 Cambio detectado en: {fname}")
            self.run_pipeline()

    def run_pipeline(self):
        print(f"🚀 Ejecutando pipeline... ({PIPELINE_SCRIPT})")
        try:
            subprocess.run(["python", PIPELINE_SCRIPT], check=True)
            print(f"✅ Pipeline ejecutado correctamente.\n")
        except subprocess.CalledProcessError as e:
            print(f"❌ Error ejecutando el pipeline: {e}")

if __name__ == "__main__":
    print(f"👁️  Watchdog activo en: {RAW_DIR}")
    print(f"📦  Ejecutando automáticamente: {PIPELINE_SCRIPT}")
    print("Esperando nuevos archivos o modificaciones...\n")

    event_handler = PipelineTrigger()
    observer = Observer()
    observer.schedule(event_handler, RAW_DIR, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Watchdog detenido por el usuario.")
        observer.stop()
    observer.join()
