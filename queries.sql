-- 1) Which movie has the highest average rating?
SELECT m.title,
       ROUND(AVG(r.rating), 2) AS avg_rating,
       COUNT(r.rating) AS num_ratings
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
GROUP BY m.movieId
HAVING COUNT(r.rating) >= 1
ORDER BY avg_rating DESC, num_ratings DESC
LIMIT 1;

-- 2) Top 5 movie genres that have the highest average rating
SELECT g.genre_name,
       ROUND(AVG(r.rating), 2) AS avg_rating,
       COUNT(r.rating) AS num_ratings
FROM genres g
JOIN movie_genres mg ON g.genre_id = mg.genre_id
JOIN ratings r ON mg.movie_id = r.movieId
GROUP BY g.genre_id
HAVING COUNT(r.rating) >= 5
ORDER BY avg_rating DESC, num_ratings DESC
LIMIT 5;

-- 3) Director with the most movies in this dataset
SELECT director,
       COUNT(*) AS movie_count
FROM movies
WHERE director IS NOT NULL AND TRIM(director) <> ''
GROUP BY director
ORDER BY movie_count DESC
LIMIT 1;

-- 4) Average rating of movies released each year
SELECT m.year AS release_year,
       ROUND(AVG(r.rating), 2) AS avg_rating,
       COUNT(r.rating) AS num_ratings
FROM movies m
JOIN ratings r ON m.movieId = r.movieId
WHERE m.year IS NOT NULL
GROUP BY m.year
ORDER BY m.year;
