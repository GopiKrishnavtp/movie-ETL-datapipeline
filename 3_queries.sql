-- queries.sql
-- This file contains analytical queries to answer the assignment questions.

-- 1. Which movie has the highest average rating?
-- We join movies and ratings, group by movie title,
-- calculate the average rating, order, and take the top 1.
SELECT
    m.title,
    AVG(r.rating) AS average_rating
FROM
    movies m
JOIN
    ratings r ON m.movie_id = r.movie_id
GROUP BY
    m.title
ORDER BY
    average_rating DESC
LIMIT 1;


-- 2. What are the top 5 movie genres that have the highest average rating?
-- This is a 3-table join. We need to link genres -> movie_genres -> ratings.
SELECT
    g.genre_name,
    AVG(r.rating) AS average_rating
FROM
    genres g
JOIN
    movie_genres mg ON g.genre_id = mg.genre_id
JOIN
    ratings r ON mg.movie_id = r.movie_id
GROUP BY
    g.genre_name
ORDER BY
    average_rating DESC
LIMIT 5;


-- 3. Who is the director with the most movies in this dataset?
-- We group by the director in the movies table, count the movies,
-- filter out any NULL directors, order, and take the top 1.
SELECT
    director,
    COUNT(movie_id) AS movie_count
FROM
    movies
WHERE
    director IS NOT NULL
GROUP BY
    director
ORDER BY
    movie_count DESC
LIMIT 1;


-- 4. What is the average rating of movies released each year?
-- We join movies and ratings, group by the release year,
-- and calculate the average rating.
SELECT
    release_year,
    AVG(r.rating) AS average_rating
FROM
    movies m
JOIN
    ratings r ON m.movie_id = r.movie_id
WHERE
    m.release_year IS NOT NULL
GROUP BY
    m.release_year
ORDER BY
    release_year DESC;

    