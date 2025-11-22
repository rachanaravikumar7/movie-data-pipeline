# Step 1: Import required libraries

import pandas as pd       # For reading and transforming CSV data
import requests           # For calling OMDb API
import sqlite3            # For connecting to SQLite database
import time               # To handle API rate limits (sleep between calls)
import os                 # To access environment variables like API key

# Step 2: Paths and constants

DB_NAME = "movies.db"           # SQLite database filename
MOVIES_CSV = "data/movies.csv"  # Path to movies CSV file
RATINGS_CSV = "data/ratings.csv" # Path to ratings CSV file

# Step 3: OMDb API Key

OMDB_API_KEY = os.getenv("OMDB_API_KEY")

if OMDB_API_KEY is None:
    raise ValueError("❌ Please set your OMDB_API_KEY environment variable before running ETL.")

# PART 2 - EXTRACT DATA

# Define data file paths inside the repository
MOVIES_CSV = "data/movies.csv"
RATINGS_CSV = "data/ratings.csv"

def extract_data():
    print("Extracting CSV data...")

    # Read movies and ratings datasets using pandas
    movies_df = pd.read_csv(MOVIES_CSV)
    ratings_df = pd.read_csv(RATINGS_CSV)

    # Show preview of extracted data
    print("Movies Data Sample:")
    print(movies_df.head())

    print("\nRatings Data Sample:")
    print(ratings_df.head())

    return movies_df, ratings_df

# PART 3 - FETCH DETAILS FROM OMDb API using Title + Year

def fetch_from_omdb(title, year=None):
    """
    Fetch movie details from OMDb API using title and optional year.
    If movie not found → return None 
    """
    # Build request parameters for OMDb API
    params = {
        "apikey": OMDB_API_KEY,
        "t": title  # search by title field
    }

    if year:  # If year available, add it to make match more accurate
        params["y"] = str(year)

    url = "https://www.omdbapi.com/"

    try:
        response = requests.get(url, params=params)
        data = response.json()

        # If found, return the details that we care about
        if data.get("Response") == "True":
            return {
                "director": data.get("Director"),
                "plot": data.get("Plot"),
                "box_office": data.get("BoxOffice"),
                "imdb_id": data.get("imdbID")
            }
        else:
            # Movie not found: return None
            return None

    except Exception as e:
        print(f"OMDb request failed for title: {title} | Error: {e}")
        return None

# PART 4 - TRANSFORM DATA (Feature Engineering Included)

import re

def transform_data(movies_df):
    print("\nTransforming data + Enriching from OMDb...")

    # Extract release year from movie title
    movies_df['year'] = movies_df['title'].str.extract(r'\((\d{4})\)').astype(float)

    # Create decade column 
    movies_df['decade'] = (movies_df['year'] // 10 * 10).astype('Int64').astype(str) + "s"

    # Remove year from title to clean it for API search
    movies_df['clean_title'] = movies_df['title'].str.replace(r'\(\d{4}\)', '', regex=True).str.strip()

    # Parse genres column into a list 
    movies_df['genres_list'] = movies_df['genres'].str.split('|')

    # New columns for OMDb enrichment
    movies_df['director'] = None
    movies_df['plot'] = None
    movies_df['box_office'] = None
    movies_df['imdb_id'] = None

    # Iterate movie rows and enrich data using OMDb
    for index, row in movies_df.iterrows():
        result = fetch_from_omdb(row['clean_title'], row['year'])

        if result:
            movies_df.at[index, 'director'] = result['director']
            movies_df.at[index, 'plot'] = result['plot']
            movies_df.at[index, 'box_office'] = result['box_office']
            movies_df.at[index, 'imdb_id'] = result['imdb_id']
        else:
            print(f"OMDb data not found for: {row['title']}")

    return movies_df

# PART 5 - LOAD DATA INTO SQLITE DATABASE

def load_data(movies_df, ratings_df):
    print("\nLoading data into SQLite database...")

    conn = sqlite3.connect('movies.db')
    cursor = conn.cursor()

    # Create movies table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movies (
            movie_id INTEGER PRIMARY KEY,
            title TEXT,
            year INTEGER,
            decade TEXT,
            director TEXT,
            plot TEXT,
            box_office TEXT,
            imdb_id TEXT
        );
    """)

    # Create ratings table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ratings (
            user_id INTEGER,
            movie_id INTEGER,
            rating REAL,
            timestamp INTEGER,
            FOREIGN KEY(movie_id) REFERENCES movies(movie_id)
        );
    """)

    # Create genres table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS genres (
            genre_id INTEGER PRIMARY KEY AUTOINCREMENT,
            genre_name TEXT UNIQUE
        );
    """)

    # Create movie_genres relationship table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS movie_genres (
            movie_id INTEGER,
            genre_id INTEGER,
            PRIMARY KEY(movie_id, genre_id),
            FOREIGN KEY(movie_id) REFERENCES movies(movie_id),
            FOREIGN KEY(genre_id) REFERENCES genres(genre_id)
        );
    """)

    # Insert movies table data
    movies_df[['movieId', 'clean_title', 'year', 'decade', 'director', 'plot', 'box_office', 'imdb_id']] \
        .rename(columns={'movieId': 'movie_id', 'clean_title': 'title'}) \
        .to_sql('movies', conn, if_exists='append', index=False)

    # Insert ratings data
    ratings_df.rename(columns={'movieId': 'movie_id'}) \
        .to_sql('ratings', conn, if_exists='append', index=False)

    # Insert genres
    genre_set = set([genre for sublist in movies_df['genres_list'] if isinstance(sublist, list) for genre in sublist])
    for genre in genre_set:
        cursor.execute("INSERT OR IGNORE INTO genres (genre_name) VALUES (?);", (genre,))

    # Insert movie-genre relationships
    for _, row in movies_df.iterrows():
        if isinstance(row['genres_list'], list):
            for genre in row['genres_list']:
                cursor.execute("""
                    INSERT OR IGNORE INTO movie_genres (movie_id, genre_id)
                    VALUES (
                        ?, (SELECT genre_id FROM genres WHERE genre_name = ?)
                    );
                """, (row['movieId'], genre))

    conn.commit()
    conn.close()
    print("Database load completed!")

if __name__ == "__main__":
    movies_df, ratings_df = extract_data()
    movies_df = transform_data(movies_df)
    load_data(movies_df, ratings_df)
