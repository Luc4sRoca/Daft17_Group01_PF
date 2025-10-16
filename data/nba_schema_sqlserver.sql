
-- NBA Star Schema for SQL Server
CREATE TABLE dim_team (
    team_id INT PRIMARY KEY,
    full_name NVARCHAR(100) NULL,
    abbreviation NVARCHAR(10) NULL,
    nickname NVARCHAR(50) NULL,
    city NVARCHAR(50) NULL,
    state NVARCHAR(50) NULL
);
CREATE TABLE dim_player (
    player_id INT PRIMARY KEY,
    full_name NVARCHAR(100) NULL,
    position NVARCHAR(20) NULL,
    height_cm FLOAT NULL,
    weight_kg FLOAT NULL,
    nationality NVARCHAR(50) NULL
);
CREATE TABLE fact_game (
    game_id INT PRIMARY KEY,
    game_date_est DATE NULL,
    season INT NULL,
    home_team_id INT NULL,
    visitor_team_id INT NULL,
    game_status_id INT NULL,
    FOREIGN KEY (home_team_id) REFERENCES dim_team(team_id),
    FOREIGN KEY (visitor_team_id) REFERENCES dim_team(team_id)
);
CREATE TABLE fact_player_game (
    game_id INT NOT NULL,
    player_id INT NOT NULL,
    team_id INT NULL,
    season INT NULL,
    points FLOAT NULL,
    assists FLOAT NULL,
    rebounds FLOAT NULL,
    minutes_played FLOAT NULL,
    PRIMARY KEY (game_id, player_id),
    FOREIGN KEY (game_id) REFERENCES fact_game(game_id),
    FOREIGN KEY (player_id) REFERENCES dim_player(player_id)
);
