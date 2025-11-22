# etl.py - Step 1: Import required libraries

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
