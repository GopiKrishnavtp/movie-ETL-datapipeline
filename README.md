##Movie Data Pipeline 
A simple ETL pipeline that ingests, cleans, and organizes movie data for analysis.
This project implements a small ETL pipeline for a movie analytics scenario. 
It extracts movie and rating data from CSV files and enriches it with details from the OMDb API. 
The data is cleaned, transformed, and loaded into a SQLite database. 
This structured database can then be used to run analytical queries on movies, genres, directors, and ratings.
Setup and Run Instructions
Install Python 3.8+ if you don’t have it already.
Install required packages using pip:
pip install -r requirements.txt
Configure OMDb API key:
Create a file named config.py in the project folder.
Add your key like this:
API_KEY = "your_omdb_api_key_here"
You can get a free key from OMDb API
Download the MovieLens dataset:
Go to https://grouplens.org/datasets/movielens/latest/
Extract movies.csv and ratings.csv into the project folder.
Run the ETL pipeline:
python etl.py


This will create the SQLite database (movies_analytics.db) and load the cleaned data.
