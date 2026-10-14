-- =====================================================================
-- Esquema Fisico del Data Warehouse (Star Schema)
-- Workshop 2: Construccion de un Pipeline de Datos Batch Confiable
-- =====================================================================

CREATE DATABASE IF NOT EXISTS music_dw;
USE music_dw;

-- 1. Dimension Artista
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_id_sk INT AUTO_INCREMENT PRIMARY KEY,
    artist_name VARCHAR(1000) NOT NULL,
    spotify_popularity INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_artist_name (artist_name(255))
) ENGINE=InnoDB;

-- 2. Dimension Fecha / Temporal
CREATE TABLE IF NOT EXISTS dim_date (
    date_id_sk INT AUTO_INCREMENT PRIMARY KEY,
    year INT NOT NULL UNIQUE,
    decade INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- 3. Dimension Genero
CREATE TABLE IF NOT EXISTS dim_genre (
    genre_id_sk INT AUTO_INCREMENT PRIMARY KEY,
    genre_name VARCHAR(100) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- 4. Tabla de Hechos: fact_tracks
CREATE TABLE IF NOT EXISTS fact_tracks (
    track_id_sk INT AUTO_INCREMENT PRIMARY KEY,
    track_id_bk VARCHAR(100) NOT NULL,
    track_name VARCHAR(1000) NOT NULL,
    artist_id_sk INT NOT NULL,
    date_id_sk INT NOT NULL,
    genre_id_sk INT NOT NULL,
    danceability FLOAT NOT NULL,
    energy FLOAT NOT NULL,
    valence FLOAT NOT NULL,
    is_grammy_winner BOOLEAN NOT NULL DEFAULT 0,
    grammy_award_category VARCHAR(500) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_fact_artist FOREIGN KEY (artist_id_sk) REFERENCES dim_artist (artist_id_sk),
    CONSTRAINT fk_fact_date FOREIGN KEY (date_id_sk) REFERENCES dim_date (date_id_sk),
    CONSTRAINT fk_fact_genre FOREIGN KEY (genre_id_sk) REFERENCES dim_genre (genre_id_sk),
    INDEX idx_bk (track_id_bk),
    INDEX idx_winner (is_grammy_winner)
) ENGINE=InnoDB;
