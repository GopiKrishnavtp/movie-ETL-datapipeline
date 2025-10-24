# etl.py

import pandas as pd
import requests
import sqlite3
from sqlalchemy import create_engine
import re
import time
import logging

# --- Configuration and Constants ---

# Import your secret API key
try:
    from config import API_KEY
except ImportError:
    print("Error: 'config.py' not found. Please create it and add your API_KEY.")
    exit()

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# File paths
MOVIES_CSV_PATH = 'movies.csv'  # Assumes 'movies.csv' is in the same folder
RATINGS_CSV_PATH = 'ratings.csv' # Assumes 'ratings.csv' is in the same folder
DB_FILE = 'movies_analytics.db'
SCHEMA_FILE = 'schema.sql'

# OMDb API settings
OMDB_API_URL = f'http://www.omdbapi.com/?apikey={API_KEY}'

# Database connection string for SQLite
DB_ENGINE = create_engine(f'sqlite:///{DB_FILE}')


def setup_database():
    """
    Reads the schema.sql file and executes it to create the database tables.
    This ensures the tables are created fresh each time (idempotent).
    """
    logging.info(f"Setting up database tables from {SCHEMA_FILE}...")
    try:
        with open(SCHEMA_FILE, 'r') as f:
            schema_sql = f.read()
        
        # Connect using sqlite3 to execute multi-statement SQL script
        with sqlite3.connect(DB_FILE) as conn:
            conn.executescript(schema_sql)
        
        logging.info("Database tables created successfully.")
            
    except FileNotFoundError:
        logging.error(f"Error: {SCHEMA_FILE} not found.")
        exit()
    except Exception as e:
        logging.error(f"Error setting up database: {e}")
        exit()

def extract_data():
    """
    Extracts data from the local CSV files and renames columns
    to match our database schema.
    """
    logging.info(f"Extracting data from {MOVIES_CSV_PATH} and {RATINGS_CSV_PATH}...")
    try:
        movies_df = pd.read_csv(MOVIES_CSV_PATH)
        ratings_df = pd.read_csv(RATINGS_CSV_PATH)
        
        # --- START FIX ---
        # Rename columns to match our SQL schema (e.g., 'movieId' -> 'movie_id')
        movies_df.rename(columns={'movieId': 'movie_id'}, inplace=True)
        ratings_df.rename(columns={'userId': 'user_id', 'movieId': 'movie_id'}, inplace=True)
        # --- END FIX ---
        
        logging.info("CSV files loaded and columns renamed successfully.")
        return movies_df, ratings_df
    
    except FileNotFoundError as e:
        logging.error(f"Error: {e}. Make sure CSV files are in the correct path.")
        exit()
    except KeyError as e:
        logging.error(f"Error renaming columns. CSV headers might be unexpected: {e}")
        exit()

def fetch_omdb_details(title, year):
    """
    Fetches additional movie details from the OMDb API.
    Handles movies not found and other API errors.
    """
    # Use 't' (title) and 'y' (year) parameters for a more accurate search
    # FIX: Ensure 'year' is a string for the API
    params = {'t': title, 'y': str(year), 'type': 'movie'}
    
    try:
        response = requests.get(OMDB_API_URL, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        data = response.json()
        
        if data.get('Response') == 'True':
            return data
        else:
            # This handles cases where the API finds no movie
            logging.warning(f"Movie not found in OMDb: {title} ({year}) - Error: {data.get('Error')}")
            return None
            
    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed for {title} ({year}): {e}")
        return None
    except Exception as e:
        logging.error(f"Error processing API response for {title} ({year}): {e}")
        return None
    
def transform_data(movies_df, ratings_df):
    """
    Cleans, transforms, and enriches the movie and rating data.
    """
    logging.info("Starting data transformation...")
    
    # --- 1. Transform 'movies' DataFrame ---
    
    # Extract year from title string using regex
    # This creates two new columns: 'title_clean' and 'release_year'
    movies_df[['title_clean', 'release_year']] = movies_df['title'].str.extract(
        r'^(.*?)\s*\((\d{4})\)$'
    )
    
    # Handle cases where regex might fail (e.g., no year in title)
    movies_df['release_year'] = pd.to_numeric(movies_df['release_year'], errors='coerce')
    movies_df['title_clean'] = movies_df['title_clean'].fillna(movies_df['title']) # Fallback to original title
    
    # Fetch API data for each movie
    logging.info("Fetching data from OMDb API... This may take several minutes.")
    api_details_list = []
    for index, row in movies_df.head(450).iterrows():
        
        # --- START FIX ---
        # Convert release_year (which is a float like 1995.0) to an
        # integer (1995) and then to a string ('1995').
        # Handle NaN values (movies with no year) gracefully.
        year_str = ''
        if pd.notna(row['release_year']):
            year_str = str(int(row['release_year']))
        # --- END FIX ---
            
        # Use the cleaned title and the corrected year string
        details = fetch_omdb_details(row['title_clean'], year_str)
        
        if details:
            api_details_list.append({
                'movie_id': row['movie_id'],
                'imdb_id': details.get('imdbID'),
                'director': details.get('Director'),
                'plot': details.get('Plot'),
                'box_office': details.get('BoxOffice'),
                'rated': details.get('Rated')
            })
        
        # Simple rate limiting to be kind to the free API
        time.sleep(0.05) # Small delay
    
    # Convert list of dicts to DataFrame
    api_details_df = pd.DataFrame(api_details_list)
    
    # Enrich: Merge API data back into the movies DataFrame
    movies_clean_df = pd.merge(movies_df, api_details_df, on='movie_id', how='left')
    
    # Clean: Handle missing values and data types
    # Clean BoxOffice: Remove '$' and ',' then convert to numeric
    movies_clean_df['box_office'] = (
        movies_clean_df['box_office']
        .replace(r'[$,]', '', regex=True)
        .pipe(pd.to_numeric, errors='coerce') # 'coerce' turns failures into NaT/NaN
    )
    
    # Keep only the columns needed for the 'movies' table
    movies_final = movies_clean_df[[
        'movie_id', 'imdb_id', 'title_clean', 'release_year', 
        'director', 'plot', 'box_office', 'rated'
    ]].rename(columns={'title_clean': 'title'}) # Rename for database
    
    
    # --- 2. Feature Engineering: 'genres' Tables (Bonus Task) ---
    
    # Split the 'genres' column by '|'
    genres_series = movies_df['genres'].str.split('|').explode('genres')
    
    # Get unique genre names
    unique_genres = genres_series.unique()
    genres_df = pd.DataFrame(unique_genres, columns=['genre_name']).dropna()
    genres_df['genre_id'] = genres_df.index # Simple integer ID
    
    # Create the 'movie_genres' linking table
    # First, create a DataFrame with movie_id and genre_name
    movie_genres_link_df = movies_df[['movie_id', 'genres']].copy()
    movie_genres_link_df['genres'] = movie_genres_link_df['genres'].str.split('|')
    movie_genres_link_df = movie_genres_link_df.explode('genres').rename(columns={'genres': 'genre_name'})
    
    # Now, merge with genres_df to get the genre_id
    movie_genres_link_df = pd.merge(
        movie_genres_link_df,
        genres_df,
        on='genre_name',
        how='inner'
    )[['movie_id', 'genre_id']] # Keep only the ID columns
    
    
    # --- 3. Transform 'ratings' DataFrame ---
    # The ratings data is already quite clean. We just select columns.
    ratings_final = ratings_df[['user_id', 'movie_id', 'rating', 'timestamp']]
    
    logging.info("Data transformation finished.")
    
    # Return all the DataFrames we need to load
    return movies_final, ratings_final, genres_df, movie_genres_link_df

def load_data(movies_df, ratings_df, genres_df, movie_genres_df):
    """
    Loads the transformed DataFrames into the SQLite database.
    Uses 'replace' to ensure idempotency (can be re-run safely).
    """
    logging.info("Loading data into the database...")
    try:
        # Note: We use DB_ENGINE (SQLAlchemy) for .to_sql()
        
        # Load data into each table
        # if_exists='replace' drops the table first and recreates it.
        # This is a simple way to ensure idempotency for this project.
        movies_df.to_sql('movies', DB_ENGINE, if_exists='replace', index=False)
        ratings_df.to_sql('ratings', DB_ENGINE, if_exists='replace', index=False)
        genres_df.to_sql('genres', DB_ENGINE, if_exists='replace', index=False)
        movie_genres_df.to_sql('movie_genres', DB_ENGINE, if_exists='replace', index=False)
        
        logging.info("Data loaded successfully.")
        
    except Exception as e:
        logging.error(f"Error loading data into database: {e}")
        exit()

# --- Main execution ---
if __name__ == "__main__":
    
    # 1. Setup the database
    setup_database()
    
    logging.info("ETL process started.")
    
    # 2. Extract
    raw_movies, raw_ratings = extract_data()
    
    # 3. Transform
    movies_to_load, ratings_to_load, genres_to_load, movie_genres_to_load = transform_data(raw_movies, raw_ratings)

    # 4. Load
    load_data(movies_to_load, ratings_to_load, genres_to_load, movie_genres_to_load)
    
    logging.info("ETL process finished successfully.")
    print("\n--- ETL Pipeline Complete ---")
    print(f"Data has been successfully loaded into '{DB_FILE}'.")