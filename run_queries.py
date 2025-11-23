import sqlite3

conn = sqlite3.connect("movies.db")
cursor = conn.cursor()

print("Running SQL queries...\n")

print("1) Movie with highest average rating:")
cursor.execute("""
SELECT m.title, ROUND(AVG(r.rating),2) AS avg_rating, COUNT(r.rating) AS num_ratings
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id
HAVING COUNT(r.rating) >= 50
ORDER BY avg_rating DESC, num_ratings DESC
LIMIT 1;
""")
print(cursor.fetchall())

print("\n2) Top 5 genres with highest average rating:")
cursor.execute("""
SELECT g.genre_name, ROUND(AVG(r.rating),2) AS avg_rating, COUNT(r.rating) AS num_ratings
FROM genres g
JOIN movie_genres mg ON g.genre_id = mg.genre_id
JOIN ratings r ON mg.movie_id = r.movie_id
GROUP BY g.genre_id
HAVING COUNT(r.rating) >= 5
ORDER BY avg_rating DESC, num_ratings DESC
LIMIT 5;
""")
print(cursor.fetchall())

print("\n3) Director with the most movies:")
cursor.execute("""
SELECT director, COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL AND TRIM(director) <> ''
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;
""")
print(cursor.fetchall())

print("\n4) Average rating of movies released each year:")
cursor.execute("""
SELECT release_year AS year, ROUND(AVG(r.rating),2) AS avg_rating, COUNT(r.rating) AS num_ratings
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
WHERE release_year IS NOT NULL
GROUP BY release_year
ORDER BY release_year;
""")
print(cursor.fetchall())

conn.close()
print("\nAll queries executed successfully!")
