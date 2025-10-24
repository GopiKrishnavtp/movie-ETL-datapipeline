-- schema.sql
-- This script defines the database schema for the movie data pipeline.

-- Drop tables if they already exist to allow for a clean re-run (idempotency)
DROP TABLE IF EXISTS movie_genres;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS movies;

-- Table to store detailed movie information
CREATE TABLE movies (
    movie_id INTEGER PRIMARY KEY,    -- From MovieLens movies.csv
    imdb_id TEXT,                    -- From OMDb API (useful for linking)
    title TEXT,
    release_year INTEGER,            -- Parsed from MovieLens title
    director TEXT,                   -- From OMDb API
    plot TEXT,                       -- From OMDb API
    box_office BIGINT,               -- From OMDb API (cleaned to numeric)
    rated TEXT                       -- From OMDb API (e.g., PG-13)
);

-- Table to store user ratings
CREATE TABLE ratings (
    rating_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    movie_id INTEGER,
    rating FLOAT,
    timestamp INTEGER,
    FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
);

-- Lookup table for genres
CREATE TABLE genres (
    genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
    genre_name TEXT UNIQUE NOT NULL  -- Ensures "Action" is only stored once
);

-- Linking table to handle the many-to-many relationship
-- (one movie can have multiple genres, one genre can have many movies)
CREATE TABLE movie_genres (
    movie_id INTEGER,
    genre_id INTEGER,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies (movie_id),
    FOREIGN KEY (genre_id) REFERENCES genres (genre_id)
);

