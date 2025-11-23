# movie-data-pipeline

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using the MovieLens dataset and OMDb API to enrich movie metadata. The final output is a normalized SQLite database (`movies.db`) along with SQL queries that analyze movies, genres, ratings, and directors.

## Project Structure

movie-data-pipeline/
├── etl.py
├── schema.sql
├── queries.sql
├── run_queries.py
└── README.md

**Input Data (inside /data folder):**
- `movies.csv` – Movie titles & genres  
- `ratings.csv` – User ratings  
- `links.csv` – Used to extract IMDb IDs (important for OMDb enrichment)

## Overview of the Solution

The pipeline performs the following steps:

 **1. Extraction**
- Reads `movies.csv`, `ratings.csv`, and `links.csv`
- Extracts MovieLens metadata
- Retrieves IMDb ID from `links.csv` and converts it to OMDb format (e.g., `tt0114709`)

 **2. Transformation**
- Splits multiple genres into a normalized structure  
- Cleans titles, formats strings, and extracts release year  
- Ensures proper data types for rating, timestamps, and IDs  

 **3. OMDb Enrichment**
Using the extracted IMDb ID, the pipeline fetches:

- Title  
- Year  
- Director  
- Runtime  
- Genre  
- Other metadata returned by OMDb  

If OMDb data is unavailable or API limit is exceeded, the movie is still processed with missing fields safely handled.

 **4. Loading**
- Creates a normalized SQLite DB based on `schema.sql`
- Loads:
  - Movies  
  - Ratings  
  - Genres  
  - Movie-Genre relationships  

 **5. SQL Analysis**
`queries.sql` includes:

1. Movie with highest average rating  
2. Top 5 genres by average rating  
3. Director with the most movies  
4. Average rating per release year  

`run_queries.py` executes these queries and prints results.

## Setup Instructions

 **1. Install dependencies**

pip install -r requirements.txt

 **2. Run ETL pipeline**

python etl.py

This creates `movies.db`.

 **3. Run SQL queries**

python run_queries.py

This prints all the analysis results in the terminal.

## Design Choices & Assumptions

 **IMDb ID through links.csv**
MovieLens `movies.csv` does NOT contain IMDb IDs.  
We extract them from `links.csv` and convert them into the OMDb-required format:

tt + zero-padded IMDb ID (7 digits)

 **Normalization**
Genres were split into:
- `genres`
- `movie_genres`

This removes duplication and ensures clean many-to-many relationships.

**Handling Missing OMDb Data**
- If OMDb API returns `"Response": "False"`, enrichment is skipped  
- Missing fields are stored as NULL  
- ETL continues without failure  

 **Limited API Calls**
To avoid API rate-limit issues while testing:
- Only first 200 movies were enriched initially  
- Once verified, full dataset can be processed  

 **Consistent Data Types**
- Ratings are floats  
- Years are integers  
- IDs are numeric  
- Genres are string-split and normalized  

## Challenges Faced & How They Were Solved

 **1. OMDb API Daily Limit**
**Problem:** API returned `"Request limit reached!"`  
**Solution:**  
- Limited the number of API calls while testing  
- Added graceful fallback when OMDb returns an error  
- Re-ran ETL next day after limit reset  

 **2. Missing Director / Year / Runtime**
Some old or foreign movies lacked complete metadata.  
**Solution:** Allowed NULL values and kept pipeline running instead of failing.

 **3. Creating Normalized Genre Tables**
The MovieLens genres field is pipe-separated.  
**Solution:**  
- Split the string  
- Deduplicate  
- Load into `genres` table  
- Use `movie_genres` for mapping  

## Final Output

The pipeline produces:

- `movies.db` – fully normalized database  
- Enriched metadata for thousands of movies  
- SQL analysis results printed to terminal  
