# Movie ETL Data Pipeline

A small ETL pipeline that extracts movie and rating data from CSV files, enriches the movie data with details from an external API (for example OMDB), cleans and transforms the data, and loads it into a SQLite database for analytics.

## Features
- Parse movie and rating CSV files.
- Enrich movie records with external metadata (e.g., runtime, overview, release date).
- Clean and normalize genres, directors, and other metadata.
- Load transformed data into a SQLite database with normalized tables for movies, genres, directors, and ratings.
- Simple query to run the ETL pipeline.

## Prerequisites
- Python 3.8+
- pip
- sqlite3 (usually included with Python)
- (Optional) API key for the external movie metadata source (TMDB or similar)

## Repository layout
(Adjust paths if your repository differs)
- data/               # Place input CSV files here (example: movies.csv, ratings.csv)
- src/ or app/        # ETL scripts and modules (e.g., run_etl.py, etl/*.py)
- requirements.txt
- README.md

## Setup

1. Clone the repository
```bash
git clone https://github.com/GopiKrishnavtp/movie-ETL-datapipeline.git
cd movie-ETL-datapipeline
```

2. Create and activate a virtual environment
```bash
python3 -m venv .venv
# .venv\Scripts\activate         # Windows PowerShell
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Configuration
- Create a `config.py` file (or update configuration) with any required environment variables, for example:
```
TMDB_API_KEY=your_omdb_api_key_here
DB_PATH=data/movies.db
DATA_DIR=data
```
- If your pipeline uses different config keys, update accordingly.

## Input data expectations
Place your CSV files in the data directory (or the folder you configure). Example expected files and columns (adapt to your CSV schema):

- movies.csv
  - movieId, title, genres
- ratings.csv
  - userId, movieId, rating, timestamp
- (optional) links.csv / tags.csv depending on your dataset

If your CSV columns differ, update the ETL script or add a mapping configuration.

## Running the ETL
Example command (adjust script name and arguments to match repo):
```bash
python run_etl.py --data-dir data --db-path data/movies_analytics.db
```
Or if the entrypoint is a module:
```bash
python -m etl.run --data-dir data --db-path data/movies_analytics.db
```

Common CLI options:
- --data-dir : directory containing CSV input files
- --db-path  : path to SQLite database file
- --api-key  : movie metadata API key (if not set in config.py)

## Database schema (example)
The ETL creates a normalized SQLite schema. Example tables:
- movies (id, title, year, runtime, overview, external_id, ...)
- genres (id, name)
- movie_genres (movie_id, genre_id)
- directors (id, name)
- movie_directors (movie_id, director_id)
- ratings (id, user_id, movie_id, rating, timestamp)

Use sqlite3 or a GUI client to inspect the DB:
```bash
sqlite3 data/movies_analytics.db
.tables
```

## Development & Testing
- Linting: run flake8/ruff (if configured)
- Tests: run pytest (if tests exist)
```bash
pytest
```

## Troubleshooting
- Missing API key: ensure OMDB_API_KEY or relevant key is set.
- CSV parsing errors: verify delimiter and encoding (UTF-8).
- Duplicate entries: check deduplication logic in the ETL scripts.
- Schema mismatches: confirm expected CSV column names and adjust mapping.

## Contributing
1. Fork the repo
2. Create a feature branch
3. Open a pull request with clear description of changes


## Contact
For issues and questions, open an issue on the repository or contact the maintainer: @GopiKrishnavtp
