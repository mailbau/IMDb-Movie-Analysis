import os
import requests
import pandas as pd
import logging
import math

def save_to_csv(movie_data_list, csv_file_path):
    df = pd.DataFrame(movie_data_list)

    try:
        # Ensure that the directory exists before saving the file
        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
        
        # Save the DataFrame to a CSV file
        df.to_csv(csv_file_path, index=False, mode='w', header=True)
        logging.info(f"Movie data successfully saved to {csv_file_path}")
    except IOError as e:
        logging.error(f"Error while saving movie data: {e}")

def fetch_top_movies(api_key, num_movies):
    try:
        # Calculate the number of pages needed based on the desired number of movies
        num_pages = math.ceil(num_movies / 20)  # TMDb API returns 20 movies per page

        top_movies = []

        for page in range(1, num_pages + 1):
            url = f'https://api.themoviedb.org/3/movie/top_rated?api_key={api_key}&language=en-US&page={page}'
            response = requests.get(url)
            response.raise_for_status()

            top_movies.extend(response.json()['results'])

        return [{'id': movie['id'], 'imdb_id': movie.get('imdb_id', '')} for movie in top_movies[:num_movies]]
    except requests.RequestException as e:
        logging.error(f"Request for top-rated movies encountered an error: {e}")
        return []

def fetch_movie_data(api_key, movie_ids):
    movie_data_list = []

    for movie_info in movie_ids:
        movie_id = movie_info['id']

        try:
            url = f'https://api.themoviedb.org/3/movie/{movie_id}?api_key={api_key}'
            response = requests.get(url)
            response.raise_for_status()

            movie_data = response.json()
            # Extract only relevant fields from the response
            extracted_data = {
                'title': movie_data.get('title', ''),
                'release_date': movie_data.get('release_date', ''),
                'overview': movie_data.get('overview', ''),
                'popularity': movie_data.get('popularity', 0),
                'vote_average': movie_data.get('vote_average', 0),
                'vote_count': movie_data.get('vote_count', 0),
                'imdb_id': movie_data.get('imdb_id', 0), 
            }
            movie_data_list.append(extracted_data)
            logging.info(f"Movie data for movie ID {movie_id} retrieved successfully")
        except requests.RequestException as e:
            logging.error(f"Request for movie ID {movie_id} encountered an error: {e}")

    return movie_data_list


def extract_movie_data(**kwargs):
    logging.basicConfig(level=logging.INFO)

    tmdb_api_key = 'd021967347b5820224a56f0b1c0496b4'  # Replace with your actual TMDB API key
    num_movies = 10 # Set the number of movies to fetch

    current_directory = os.path.dirname(os.path.abspath(__file__))
    raw_data_folder = os.path.join(current_directory, '../raw_data')
    csv_file_path = os.path.join(raw_data_folder, 'movie_data.csv')

    movie_ids = fetch_top_movies(tmdb_api_key, num_movies)

    if movie_ids:
        movie_data_list = fetch_movie_data(tmdb_api_key, movie_ids)
        save_to_csv(movie_data_list, csv_file_path)

if __name__ == "__main__":
    extract_movie_data()
