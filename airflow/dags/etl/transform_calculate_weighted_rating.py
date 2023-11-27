import os
import pandas as pd

def transform_calculate_weighted_rating():
    # Get the absolute path of the current script
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Load the CSV into a pandas DataFrame
    csv_path = os.path.join(current_directory, 'raw_data', 'movie_data.csv')
    df = pd.read_csv(csv_path)

    # Set a threshold for the minimum number of votes
    min_votes = 1000

    # Calculate the weighted rating
    df['weighted_rating'] = (df['vote_count'] / (df['vote_count'] + min_votes)) * df['vote_average'] + (min_votes / (df['vote_count'] + min_votes)) * df['vote_average'].mean()

    # Save the updated DataFrame back to a CSV file
    transformed_csv_path = os.path.join(current_directory, 'movie_data_transformed.csv')
    df.to_csv(transformed_csv_path, index=False)

if __name__ == '__main__':
    transform_calculate_weighted_rating()
