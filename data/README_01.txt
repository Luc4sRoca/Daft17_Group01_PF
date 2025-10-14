🏀 Proyecto Final – Análisis Predictivo del Rendimiento en la NBA
📘 Descripción General

Este proyecto forma parte del Proyecto Final de la carrera Data Analytics en SoyHenry.
El objetivo es evaluar y predecir cómo el rendimiento de los jugadores impacta en la probabilidad de victoria de los equipos, identificando las métricas estadísticas más relevantes.

El flujo de trabajo combina Python (ETL + EDA), SQL Server (almacenamiento DWH) y Power BI (visualización e interacción).

🧠 Objetivos del Proyecto

Analizar la eficiencia ofensiva y defensiva de los equipos.

Identificar correlaciones entre métricas individuales y resultados de partidos.

Desarrollar un modelo predictivo (regresión lineal o logística) para estimar la probabilidad de victoria.

Crear un dashboard interactivo en Power BI con KPIs, segmentadores y storytelling.

📊 Estructura de Datos y ETL

El dataset base proviene de Kaggle – Basketball Dataset (NBA)
.
Durante el proceso se seleccionaron y consolidaron las siguientes tablas principales:

Tabla fuente	Descripción breve
game	Estadísticas por partido (local y visitante).
team	Información base de equipos (nombre, ciudad, año de fundación).
player	Identificación y estado de jugadores.
common_player_info	Datos demográficos y trayectoria de jugadores.
other_stats	Métricas avanzadas por partido (rebotes, turnovers, puntos por jugada).
game_summary	Información general y logística del partido.

Se realizaron uniones (joins) progresivas para enriquecer la base:

player + common_player_info → player_enriched

game + team → game_team

game_team + other_stats → game_team_other_stats

El resultado es un modelo en estrella (star schema) con game_team_other_stats como tabla de hechos, y team, player_enriched como dimensiones.

🧹 Limpieza y Transformación (ETL)

El proceso de limpieza se implementó en el script principal del notebook con las siguientes etapas:

Eliminación de redundancias: columnas repetidas o sin valor analítico (e.g. display_first_last, team_abbreviation).

Creación de campos derivados:

edad (calculada desde birthdate)

años_jugando (to_year - from_year)

undrafted (jugadores sin selección en el draft)

Manejo de nulos con función limpiar_nulos(df):

Columnas con >80% de nulos fueron descartadas.

Identificadores sin valor fueron eliminados.

Valores numéricos completados con mediana o 0.

Valores categóricos reemplazados por “Desconocido”.

Tipificación controlada:

IDs de equipos (team_id_home, team_id_away) forzados a tipo Int64 para evitar DtypeWarning.

Los resultados limpios fueron exportados a la carpeta data/final/ como archivos *_clean.csv.

🔍 Análisis Exploratorio de Datos (EDA)

El EDA se realizó sobre las tablas limpias, incluyendo:

Cálculo de porcentaje de nulos y validación de consistencia.

Estadísticas descriptivas (describe()) por columna numérica.

Análisis de métricas clave:

Distribución de puntos (pts_home, pts_away)

Eficiencia de tiro (fg_pct_home, fg3_pct_home, ft_pct_home)

Comparativa de rebotes y asistencias.

Detección de outliers en métricas de desempeño.

🧾 Cambios Principales Implementados

Integración de Player + Common_Player_Info → player_enriched consolidado con cálculo de edad y trayectoria.

Limpieza automatizada por función genérica para mantener consistencia entre datasets.

Normalización de tipos de datos en columnas críticas (Int64 en IDs).

Exportación ordenada por capas:

/data/raw → fuentes originales

/data/clean → post limpieza manual

/data/final → datasets procesados y listos para SQL o Power BI

EDA automatizado mediante bucles for y while para análisis por columna.

🧭 Próximos Pasos

Carga de las tablas finales en SQL Server con esquema en estrella.

Modelado predictivo (Regresión Lineal / Logística) con scikit-learn.

Creación de dashboard en Power BI con KPIs:

Puntos promedio por jugador y equipo

Asistencias y rebotes promedio

Porcentaje de victorias

Comparativa de desempeño local vs visitante

🧩 Tecnologías Utilizadas

Python: Pandas, NumPy, Matplotlib, Seaborn

SQL Server: Almacenamiento DWH

Power BI: Visualización interactiva

GitHub: Control de versiones y trabajo colaborativo

👥 Equipo de Desarrollo

Juan Lucas Roca Vajdik – Data Analyst

Percy Ignacio Marzoratti Hill – Data Analyst