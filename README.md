# Movie Data Pipeline

A simple ETL pipeline that ingests, cleans, and organizes movie data for analysis.
This project implements a small ETL pipeline for a movie analytics scenario.
It extracts movie and rating data from CSV files and enriches it with details from the OMDb API.
The data is cleaned, transformed, and loaded into a SQLite database.
This structured database can then be used to run analytical queries on movies, genres, directors, and ratings.

# Setup and Run Instructions

1. Install Python 3.8+ and required packages:
   * pip install -r requirements.txt

2. Create config.py with your OMDb API key:
   * API_KEY = "your_omdb_api_key_here"

3. Download the MovieLens dataset:
   * Go to https://grouplens.org/datasets/movielens/latest/
   * Extract movies.csv and ratings.csv into the project folder.
   * Place movies.csv and ratings.csv in the project folder.
   * Run the ETL pipeline:python etl.py

# Design Choices & Assumptions

1. Database: Used SQLite for simplicity and easy portability.

2. Schema Design: 
   * Normalized tables – movies, ratings, genres, and movie_genres – to handle many-to-many relationships and avoid duplicate data.

3. ETL Approach:
   * Extract: Read CSVs and fetch additional movie details from the OMDb API.
   * Transform: Clean missing values, parse release year, normalize genres, convert BoxOffice to numeric.
   * Load: Insert into database with replace to ensure idempotency.

4. Assumptions:
   * Movie titles may not exactly match API entries, so release year is included for better matching.
   * Missing API fields are allowed and handled gracefully.
   * The dataset is small, so SQLite is sufficient; for larger datasets, a more robust RDBMS could be used.

# Challenges & How I Overcame Them
1. Inconsistent movie titles in the OMDb API: 
   * Cleaned titles and included release year to improve matching.

2. Missing or incomplete API data: 
   * Handled gracefully by allowing None values and logging warnings.

3. BoxOffice values in different formats: 
   * Used regex to remove $ and , and converted to numeric.

4. API rate limits: 
   * Added a small delay (time.sleep(0.05)) between requests.

5. Ensuring idempotency: 
   * Used DROP TABLE IF EXISTS and if_exists='replace' to safely reload data.  

# Design
![graph](graphdata.jpg)

movie-data-pipeline/           # Root project folder
├── etl.py                     # Main ETL script (extract, transform, load)
├── schema.sql                  # Database schema (tables creation)
├── queries.sql                 # SQL queries for analysis
├── README.md                   # Project overview, setup, design, challenges
├── requirements.txt            # Python dependencies
├── config.py                   # OMDb API key (user-created by you)
├── movies.csv                  # MovieLens movies dataset
└── ratings.csv                 # MovieLens ratings dataset  

