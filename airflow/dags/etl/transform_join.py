import os
import pandas as pd

def transform_join():
    

    # Get the absolute path of the current script
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Load the movie_data_transformed CSV into a pandas DataFrame
    csv_path = os.path.join(current_directory, 'movie_data_transformed.csv')
    df_movies = pd.read_csv(csv_path)

    # Load the user_reviews_transformed CSV into a pandas DataFrame
    csv_path = os.path.join(current_directory, 'user_reviews_transformed.csv')
    df_reviews = pd.read_csv(csv_path)

    # Merge the datasets on the common column 'imdb_id'
    df_combined = pd.merge(df_movies, df_reviews, on='imdb_id', how='inner')

    # Save the updated DataFrame to a new CSV file
    combined_csv_path = os.path.join(current_directory, 'combined_data.csv')
    df_combined.to_csv(combined_csv_path, index=False)

if __name__ == '__main__':
    transform_join()