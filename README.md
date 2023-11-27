# IMDb-Movie-Analysis
End-to-end data engineering ETL pipeline using movie data from TMDb API and scraped user reviews from IMDb web

## ETL Process Overview

### Extract
1. Fetch movie data using TMDb API. Number of movies fetched can be set by changing num_movies in extract_movie_data.py
2. Scrape user reviews from IMDb web by using the IMDb id of the fetched movies
### Transform
1. Ensure format consistency of movie release date
2. Calculate weighted rating of each movie and add to a new column in the movie data
3. Apply sentiment analysis to the user reviews and add to a new column in the user reviews
4. Look for top keywords in each movies' reviews and add to a new column in the user reviews
5. Join the transformed movie data csv with the transformed user reviews csv
### Load
1. Load to postgresql database

## Orchestration
The ETL Pipeline is then orchestrated and scheduled using the DAG movie_analysis_etl_pipeline.py and executing it in airflow
