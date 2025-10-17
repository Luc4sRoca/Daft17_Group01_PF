CREATE DATABASE NBA_Project;
GO
USE NBA_Project;
go

/* =========================================================
   0.A) PRE-FLIGHT: modo seguro y esquema lógico
   ========================================================= */
SET XACT_ABORT ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'nba')
    EXEC('CREATE SCHEMA nba');
GO


/* =========================================================
   0.C) VISTA RÁPIDA: Tablas y columnas (para chequear nombres)
   ========================================================= */
-- Tablas con cantidad de filas (estimada) y esquema
SELECT t.TABLE_SCHEMA AS Esquema,
       t.TABLE_NAME   AS Tabla
FROM INFORMATION_SCHEMA.TABLES t
WHERE t.TABLE_TYPE='BASE TABLE'
ORDER BY 1,2;

-- Columnas de cada tabla
SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
GO

/* =========================================================
   1) AUDITORÍA INICIAL: duplicados y nulos en claves
   ========================================================= */
-- Duplicados en claves candidatas (debería dar 0 en todas)
SELECT 'team_final' AS tabla, COUNT(*)-COUNT(DISTINCT team_id) AS duplicados FROM dbo.team_final
UNION ALL SELECT 'player_final', COUNT(*)-COUNT(DISTINCT player_id) FROM dbo.player_final
UNION ALL SELECT 'common_player_info_final', COUNT(*)-COUNT(DISTINCT player_id) FROM dbo.common_player_info_final
UNION ALL SELECT 'game_summary_final', COUNT(*)-COUNT(DISTINCT game_id) FROM dbo.game_summary_final
UNION ALL SELECT 'game_final', COUNT(*)-COUNT(DISTINCT game_id) FROM dbo.game_final
UNION ALL SELECT 'other_stats_final', COUNT(*)-COUNT(DISTINCT game_id) FROM dbo.other_stats_final;

-- Nulos en claves (debería dar 0 en todas)
SELECT 'team_final' tabla, COUNT(*) nulos FROM dbo.team_final WHERE team_id IS NULL
UNION ALL SELECT 'player_final', COUNT(*) FROM dbo.player_final WHERE player_id IS NULL
UNION ALL SELECT 'common_player_info_final', COUNT(*) FROM dbo.common_player_info_final WHERE player_id IS NULL
UNION ALL SELECT 'game_summary_final', COUNT(*) FROM dbo.game_summary_final WHERE game_id IS NULL
UNION ALL SELECT 'game_final', COUNT(*) FROM dbo.game_final WHERE game_id IS NULL
UNION ALL SELECT 'other_stats_final', COUNT(*) FROM dbo.other_stats_final WHERE game_id IS NULL;
GO

/* =========================================================
   2) NORMALIZAR TIPOS (si falla, limpiar datos antes)
   ========================================================= */
BEGIN TRY ALTER TABLE dbo.game_summary_final ALTER COLUMN game_date_est DATE; END TRY BEGIN CATCH END CATCH;

BEGIN TRY ALTER TABLE dbo.team_final ALTER COLUMN team_id INT; END TRY BEGIN CATCH END CATCH;
BEGIN TRY ALTER TABLE dbo.player_final ALTER COLUMN player_id INT; END TRY BEGIN CATCH END CATCH;
BEGIN TRY ALTER TABLE dbo.common_player_info_final ALTER COLUMN player_id INT; END TRY BEGIN CATCH END CATCH;

BEGIN TRY ALTER TABLE dbo.game_final ALTER COLUMN game_id BIGINT; END TRY BEGIN CATCH END CATCH;
BEGIN TRY ALTER TABLE dbo.other_stats_final ALTER COLUMN game_id BIGINT; END TRY BEGIN CATCH END CATCH;

BEGIN TRY ALTER TABLE dbo.game_final ALTER COLUMN team_id_home INT; END TRY BEGIN CATCH END CATCH;
BEGIN TRY ALTER TABLE dbo.game_final ALTER COLUMN team_id_away INT; END TRY BEGIN CATCH END CATCH;
BEGIN TRY ALTER TABLE dbo.other_stats_final ALTER COLUMN team_id_home INT; END TRY BEGIN CATCH END CATCH;
BEGIN TRY ALTER TABLE dbo.other_stats_final ALTER COLUMN team_id_away INT; END TRY BEGIN CATCH END CATCH;
GO

/* =========================================================
   3) CREAR PKs (saltea si ya existen)
   ========================================================= */
IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [name] = 'PK_team_final')
    ALTER TABLE dbo.team_final ADD CONSTRAINT PK_team_final PRIMARY KEY (team_id);

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [name] = 'PK_player_final')
    ALTER TABLE dbo.player_final ADD CONSTRAINT PK_player_final PRIMARY KEY (player_id);

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [name] = 'PK_common_player_info_final')
    ALTER TABLE dbo.common_player_info_final ADD CONSTRAINT PK_common_player_info_final PRIMARY KEY (player_id);

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [name] = 'PK_game_summary_final')
    ALTER TABLE dbo.game_summary_final ADD CONSTRAINT PK_game_summary_final PRIMARY KEY (game_id);

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [name] = 'PK_game_final')
    ALTER TABLE dbo.game_final ADD CONSTRAINT PK_game_final PRIMARY KEY (game_id);

IF NOT EXISTS (SELECT 1 FROM sys.key_constraints WHERE [name] = 'PK_other_stats_final')
    ALTER TABLE dbo.other_stats_final ADD CONSTRAINT PK_other_stats_final PRIMARY KEY (game_id);
GO

/* =========================================================
   4) LIMPIEZA DE HUÉRFANOS (equipos inválidos) + EQUIPO "UNKNOWN"
   ========================================================= */
-- Crear equipo Unknown si no existe
IF NOT EXISTS (SELECT 1 FROM dbo.team_final WHERE team_id = 0)
INSERT INTO dbo.team_final (team_id, abbreviation, full_name, city)
VALUES (0, 'UNK', 'Unknown Team', 'N/A');

-- Reemplazar IDs inválidos (fuera del rango oficial NBA) por 0 en tablas de juego
UPDATE dbo.game_final
SET team_id_home = 0
WHERE team_id_home IS NULL
   OR team_id_home NOT BETWEEN 1610612737 AND 1610612766;

UPDATE dbo.game_final
SET team_id_away = 0
WHERE team_id_away IS NULL
   OR team_id_away NOT BETWEEN 1610612737 AND 1610612766;

UPDATE dbo.other_stats_final
SET team_id_home = 0
WHERE team_id_home IS NULL
   OR team_id_home NOT BETWEEN 1610612737 AND 1610612766;

UPDATE dbo.other_stats_final
SET team_id_away = 0
WHERE team_id_away IS NULL
   OR team_id_away NOT BETWEEN 1610612737 AND 1610612766;

-- Si common_player_info_final tiene team_id y está fuera de catálogo, también a 0
IF COL_LENGTH('dbo.common_player_info_final','team_id') IS NOT NULL
BEGIN
    UPDATE dbo.common_player_info_final
    SET team_id = 0
    WHERE team_id IS NULL
       OR team_id NOT IN (SELECT team_id FROM dbo.team_final);
END
GO

/* =========================================================
   4.1) CHEQUEO HUÉRFANOS (debe devolver 0 filas)
   ========================================================= */
SELECT 'game_final.team_id_home' origen, gf.team_id_home AS team_id, COUNT(*) n
FROM dbo.game_final gf
LEFT JOIN dbo.team_final t ON t.team_id = gf.team_id_home
WHERE t.team_id IS NULL
GROUP BY gf.team_id_home
UNION ALL
SELECT 'game_final.team_id_away', gf.team_id_away, COUNT(*)
FROM dbo.game_final gf
LEFT JOIN dbo.team_final t ON t.team_id = gf.team_id_away
WHERE t.team_id IS NULL
GROUP BY gf.team_id_away
UNION ALL
SELECT 'other_stats_final.team_id_home', os.team_id_home, COUNT(*)
FROM dbo.other_stats_final os
LEFT JOIN dbo.team_final t ON t.team_id = os.team_id_home
WHERE t.team_id IS NULL
GROUP BY os.team_id_home
UNION ALL
SELECT 'other_stats_final.team_id_away', os.team_id_away, COUNT(*)
FROM dbo.other_stats_final os
LEFT JOIN dbo.team_final t ON t.team_id = os.team_id_away
WHERE t.team_id IS NULL
GROUP BY os.team_id_away;
GO

/* =========================================================
   5) CREAR FKs (solo si no existen) — ahora sin huérfanos
   ========================================================= */
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE [name] = 'FK_game_final_team_home')
ALTER TABLE dbo.game_final
ADD CONSTRAINT FK_game_final_team_home
FOREIGN KEY (team_id_home) REFERENCES dbo.team_final(team_id);

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE [name] = 'FK_game_final_team_away')
ALTER TABLE dbo.game_final
ADD CONSTRAINT FK_game_final_team_away
FOREIGN KEY (team_id_away) REFERENCES dbo.team_final(team_id);

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE [name] = 'FK_other_stats_team_home')
ALTER TABLE dbo.other_stats_final
ADD CONSTRAINT FK_other_stats_team_home
FOREIGN KEY (team_id_home) REFERENCES dbo.team_final(team_id);

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE [name] = 'FK_other_stats_team_away')
ALTER TABLE dbo.other_stats_final
ADD CONSTRAINT FK_other_stats_team_away
FOREIGN KEY (team_id_away) REFERENCES dbo.team_final(team_id);

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE [name] = 'FK_game_summary_game')
ALTER TABLE dbo.game_summary_final
ADD CONSTRAINT FK_game_summary_game
FOREIGN KEY (game_id) REFERENCES dbo.game_final(game_id);

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE [name] = 'FK_common_player_info_player')
ALTER TABLE dbo.common_player_info_final
ADD CONSTRAINT FK_common_player_info_player
FOREIGN KEY (player_id) REFERENCES dbo.player_final(player_id);

-- common_player_info_final -> team_final (recrear limpia si existía con conflictos)
IF EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_common_player_info_team')
    ALTER TABLE dbo.common_player_info_final DROP CONSTRAINT FK_common_player_info_team;
ALTER TABLE dbo.common_player_info_final
ADD CONSTRAINT FK_common_player_info_team
FOREIGN KEY (team_id) REFERENCES dbo.team_final(team_id);
GO

/* =========================================================
   6) DIMENSIONES: DimDate y DimGame
   ========================================================= */
IF OBJECT_ID('nba.DimDate') IS NULL
BEGIN
    CREATE TABLE nba.DimDate (
        DateKey       INT PRIMARY KEY,   -- yyyymmdd
        [Date]        DATE NOT NULL,
        [Year]        INT  NOT NULL,
        [Month]       TINYINT NOT NULL,
        [Day]         TINYINT NOT NULL,
        [MonthName]   VARCHAR(12) NOT NULL,
        [Quarter]     TINYINT NOT NULL,
        [Weekday]     TINYINT NOT NULL,
        [WeekdayName] VARCHAR(12) NOT NULL
    );
END;

;WITH d AS (
  SELECT DISTINCT game_date_est AS d
  FROM dbo.game_summary_final
  WHERE game_date_est IS NOT NULL
)
INSERT INTO nba.DimDate (DateKey,[Date],[Year],[Month],[Day],[MonthName],[Quarter],[Weekday],[WeekdayName])
SELECT  CONVERT(INT, FORMAT(d,'yyyyMMdd')),
        d,
        YEAR(d),
        MONTH(d),
        DAY(d),
        DATENAME(MONTH,d),
        DATEPART(QUARTER,d),
        DATEPART(WEEKDAY,d),
        DATENAME(WEEKDAY,d)
FROM d
WHERE CONVERT(INT, FORMAT(d,'yyyyMMdd')) NOT IN (SELECT DateKey FROM nba.DimDate);
GO

IF OBJECT_ID('nba.DimGame') IS NULL
BEGIN
  CREATE TABLE nba.DimGame (
      game_id     BIGINT PRIMARY KEY,
      game_date   DATE NOT NULL,
      DateKey     INT NOT NULL,
      game_status_id INT NULL,
      game_status_text VARCHAR(50) NULL
  );
END;

MERGE nba.DimGame AS tgt
USING (
  SELECT gs.game_id,
         gs.game_date_est AS game_date,
         CONVERT(INT, FORMAT(gs.game_date_est,'yyyyMMdd')) AS DateKey,
         gs.game_status_id,
         gs.game_status_text
  FROM dbo.game_summary_final gs
) AS src
ON tgt.game_id = src.game_id
WHEN MATCHED THEN UPDATE SET
    tgt.game_date = src.game_date,
    tgt.DateKey   = src.DateKey,
    tgt.game_status_id = src.game_status_id,
    tgt.game_status_text = src.game_status_text
WHEN NOT MATCHED BY TARGET THEN
    INSERT (game_id, game_date, DateKey, game_status_id, game_status_text)
    VALUES (src.game_id, src.game_date, src.DateKey, src.game_status_id, src.game_status_text);
GO

/* =========================================================
   7) VISTAS DEL HECHO (modelo estrella) — grano equipo/partido
   ========================================================= */
CREATE OR ALTER VIEW nba.vw_FactGameTeam AS
SELECT 
    gf.game_id,
    dg.game_date,
    dg.DateKey,
    t_home.team_id       AS team_id,
    'HOME'               AS side,
    gf.team_abbreviation_home AS team_abbr,
    gf.team_name_home    AS team_name,
    gf.pts_home          AS pts,
    gf.fgm_home          AS fgm,
    gf.fga_home          AS fga,
    gf.fg3m_home         AS fg3m,
    gf.ftm_home          AS ftm,
    gf.fta_home          AS fta,
    gf.oreb_home         AS oreb,
    gf.dreb_home         AS dreb,
    gf.ast_home          AS ast,
    gf.tov_home          AS tov,
    gf.blk_home          AS blk,
    gf.stl_home          AS stl,
    gf.plus_minus_home   AS plus_minus
FROM dbo.game_final gf
JOIN nba.DimGame dg ON dg.game_id = gf.game_id
JOIN dbo.team_final t_home ON t_home.team_id = gf.team_id_home
UNION ALL
SELECT 
    gf.game_id,
    dg.game_date,
    dg.DateKey,
    t_away.team_id       AS team_id,
    'AWAY'               AS side,
    gf.team_abbreviation_away,
    gf.team_name_away,
    gf.pts_away,
    gf.fgm_away,
    gf.fga_away,
    gf.fg3m_away,
    gf.ftm_away,
    gf.fta_away,
    gf.oreb_away,
    gf.dreb_away,
    gf.ast_away,
    gf.tov_away,
    gf.blk_away,
    gf.stl_away,
    gf.plus_minus_away
FROM dbo.game_final gf
JOIN nba.DimGame dg ON dg.game_id = gf.game_id
JOIN dbo.team_final t_away ON t_away.team_id = gf.team_id_away;
GO

CREATE OR ALTER VIEW nba.vw_FactOtherStatsTeam AS
SELECT 
    os.game_id,
    dg.game_date,
    dg.DateKey,
    os.team_id_home AS team_id,
    'HOME' AS side,
    os.team_turnovers_home      AS turnovers,
    os.team_rebounds_home       AS rebounds,
    os.pts_off_to_home          AS pts_off_turnovers
FROM dbo.other_stats_final os
JOIN nba.DimGame dg ON dg.game_id = os.game_id
UNION ALL
SELECT 
    os.game_id,
    dg.game_date,
    dg.DateKey,
    os.team_id_away AS team_id,
    'AWAY' AS side,
    os.team_turnovers_away,
    os.team_rebounds_away,
    os.pts_off_to_away
FROM dbo.other_stats_final os
JOIN nba.DimGame dg ON dg.game_id = os.game_id;
GO

-- (Opcional) vista con bandera de victoria
CREATE OR ALTER VIEW nba.vw_FactGameTeam_WithWin AS
WITH sides AS (
  SELECT
    f.*,
    CASE WHEN side = 'HOME'
         THEN (SELECT TOP 1 f2.pts FROM nba.vw_FactGameTeam f2 WHERE f2.game_id=f.game_id AND f2.side='AWAY')
         ELSE (SELECT TOP 1 f2.pts FROM nba.vw_FactGameTeam f2 WHERE f2.game_id=f.game_id AND f2.side='HOME')
    END AS opp_pts
  FROM nba.vw_FactGameTeam f
)
SELECT *,
       CASE WHEN pts > opp_pts THEN 1 WHEN pts < opp_pts THEN 0 ELSE NULL END AS win
FROM sides;
GO

/* =========================================================
   8) ÍNDICES ÚTILES
   ========================================================= */
CREATE INDEX IX_game_final_team_home   ON dbo.game_final(team_id_home);
CREATE INDEX IX_game_final_team_away   ON dbo.game_final(team_id_away);
CREATE INDEX IX_other_stats_team_home  ON dbo.other_stats_final(team_id_home);
CREATE INDEX IX_other_stats_team_away  ON dbo.other_stats_final(team_id_away);
CREATE INDEX IX_game_summary_date      ON dbo.game_summary_final(game_date_est);
CREATE INDEX IX_DimGame_DateKey        ON nba.DimGame(DateKey);
CREATE INDEX IX_DimDate_Date           ON nba.DimDate([Date]);
GO

/* =========================================================
   9) QA FINAL: todo sano
   ========================================================= */
-- PKs creadas
SELECT t.name AS Tabla, kc.name AS Nombre_PK
FROM sys.key_constraints kc
JOIN sys.tables t ON kc.parent_object_id = t.object_id
WHERE kc.[type] = 'PK'
ORDER BY t.name;

-- FKs creadas
SELECT fk.name AS Nombre_FK, OBJECT_NAME(fk.parent_object_id) AS Tabla_Origen,
       OBJECT_NAME(fk.referenced_object_id) AS Tabla_Ref
FROM sys.foreign_keys fk
ORDER BY 1;

-- Dimensiones con datos
SELECT COUNT(*) AS Fechas FROM nba.DimDate;
SELECT COUNT(*) AS Juegos FROM nba.DimGame;

-- Vistas de hechos: dos filas por game_id
SELECT TOP 10 * FROM nba.vw_FactGameTeam;
SELECT game_id, COUNT(*) AS filas_por_partido
FROM nba.vw_FactGameTeam
GROUP BY game_id
HAVING COUNT(*) <> 2;  -- debería devolver 0 filas

-- Huérfanos: deberían ser 0
SELECT COUNT(*) AS Huerfanos_gf_home
FROM dbo.game_final gf LEFT JOIN dbo.team_final t ON t.team_id = gf.team_id_home
WHERE t.team_id IS NULL;

SELECT COUNT(*) AS Huerfanos_gf_away
FROM dbo.game_final gf LEFT JOIN dbo.team_final t ON t.team_id = gf.team_id_away
WHERE t.team_id IS NULL;

SELECT COUNT(*) AS Huerfanos_os_home
FROM dbo.other_stats_final os LEFT JOIN dbo.team_final t ON t.team_id = os.team_id_home
WHERE t.team_id IS NULL;

SELECT COUNT(*) AS Huerfanos_os_away
FROM dbo.other_stats_final os LEFT JOIN dbo.team_final t ON t.team_id = os.team_id_away
WHERE t.team_id IS NULL;

-- Sanidad básica de métricas
SELECT TOP 20 * FROM nba.vw_FactGameTeam WHERE pts IS NULL OR pts < 0;
GO
