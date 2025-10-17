PROYECTO FINAL – ANÁLISIS DEL RENDIMIENTO NBA

DESCRIPCIÓN GENERAL
Proyecto final del bootcamp Data Analytics – Soy Henry.
Objetivo: analizar cómo las métricas de juego y eficiencia por equipo impactan en los resultados,
utilizando un modelo de datos limpio y relacional listo para SQL Server y Power BI.

OBJETIVOS
- Analizar desempeño ofensivo/defensivo por equipo.
- Detectar patrones de rendimiento y su relación con victorias.
- Desarrollar dashboard interactivo con KPIs clave (puntos, rebotes, asistencias, eficacia, etc.).

ESTRUCTURA DE DATOS (MODELO ESTRELLA)

Tabla               | Tipo | Descripción breve
-------------------- | ---- | ---------------------------------------------
fact_game           | Hecho | Datos generales de cada partido (fecha, temporada, equipos, marcador).
fact_team_game      | Hecho | Estadísticas por equipo y partido (dos filas: home / away).
other_stats_final   | Hecho – backup | Métricas agregadas por juego (estructura wide, columnas *_home y *_away).
dim_team            | Dimensión | Información descriptiva de los equipos.
dim_player          | Dimensión | Información básica de los jugadores.
game_summary        | Dimensión auxiliar | Detalles logísticos del partido (estadio, árbitros, público).

LIMPIEZA Y TRANSFORMACIÓN (ETL)
- Estandarización de nombres de columnas (snake_case).
- Tipificación controlada (IDs en Int64, fechas en datetime64).
- Deduplicación por PK natural (game_id, team_id, etc.).
- Mapeo de IDs históricos (team_xref.csv) para integridad de claves foráneas.
- Normalización de métricas de other_stats_final → fact_team_game_final.
- Exportación final a /data/final/ lista para SQL Server y Power BI.

VALIDACIONES
- PKs sin duplicados en todas las tablas.
- FKs validadas al 98–100 % entre hechos y dimensiones.
- Sin nulos en columnas críticas.
- Fechas coherentes con temporadas.
- Detección automática de nuevos datos (automatización de ingesta).

ENTORNO ANALÍTICO
Python: ETL, validaciones, automatización.
SQL Server: almacenamiento DWH (modelo estrella).
Power BI: visualización e interacción (KPIs y storytelling).

EQUIPO
Juan Lucas Roca Vajdik – Data Analyst
Percy Ignacio Marzoratti Hill – Data Analyst
