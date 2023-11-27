import os
import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup

def save_to_csv(movie_data_list, movie_csv_file_path, user_reviews_csv_file_path):
    df = pd.DataFrame(movie_data_list)

    try:
        # Save the movie data to a CSV file without the 'user_reviews' column
        df.drop(columns=['user_reviews']).to_csv(movie_csv_file_path, index=False, mode='w', header=True)
        logging.info(f"Movie data successfully saved to {movie_csv_file_path}")

        # Extract user reviews and save to a separate CSV file
        user_reviews_df = pd.DataFrame({'imdb_id': df['imdb_id'], 'user_reviews': df['user_reviews']})
        user_reviews_df.to_csv(user_reviews_csv_file_path, index=False, mode='w', header=True)
        logging.info(f"User reviews successfully saved to {user_reviews_csv_file_path}")

    except IOError as e:
        logging.error(f"Error while saving data: {e}")

def fetch_user_reviews(imdb_id):
    if pd.notna(imdb_id):  # Check if IMDb ID is not 'nan'
        url = f'https://www.imdb.com/title/{imdb_id}/reviews'
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')

            # Extract user reviews
            reviews = []
            review_elements = soup.find_all('div', class_='text show-more__control')
            for review_element in review_elements:
                reviews.append(review_element.get_text(strip=True))

            return reviews
        except requests.RequestException as e:
            print(f"Request for user reviews encountered an error: {e}")
            return []
    else:
        return []  # Skip processing for movies without IMDb IDs

def extract_imdb_reviews():
    logging.basicConfig(level=logging.INFO)

    current_directory = os.path.dirname(os.path.abspath(__file__))
    raw_data_folder = os.path.join(current_directory, 'raw_data')
    
    movie_csv_file_path = os.path.join(raw_data_folder, 'movie_data.csv')
    user_reviews_csv_file_path = os.path.join(raw_data_folder, 'user_reviews.csv')

    # Read movie data from the existing CSV file
    movie_data_list = pd.read_csv(movie_csv_file_path).to_dict(orient='records')

    for movie_data in movie_data_list:
        imdb_id = movie_data.get('imdb_id', '')

        if imdb_id:
            user_reviews = fetch_user_reviews(imdb_id)
            movie_data['user_reviews'] = user_reviews
            logging.info(f"User reviews for IMDb ID {imdb_id} retrieved successfully")
        else:
            logging.warning("IMDb ID not found or set to 'nan' for a movie. Skipping review fetching for this movie.")

    # Update the CSV files with movie data and user reviews
    save_to_csv(movie_data_list, movie_csv_file_path, user_reviews_csv_file_path)

if __name__ == "__main__":
    extract_imdb_reviews()
