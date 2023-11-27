import os
import pandas as pd

def transform_date_format():
    # Get the absolute path of the current script
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Load the CSV into a pandas DataFrame
    csv_path = os.path.join(current_directory, 'raw_data', 'movie_data.csv')
    df = pd.read_csv(csv_path)

    # Convert the 'release_date' column to datetime format
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

    # Convert the 'release_date' column to the standard YYYY-MM-DD format
    df['release_date'] = df['release_date'].dt.strftime('%Y-%m-%d')

    # Save the updated DataFrame back to a CSV file
    transformed_csv_path = os.path.join(current_directory, 'movie_data_transformed.csv')
    df.to_csv(transformed_csv_path, index=False)

if __name__ == '__main__':
    transform_date_format()
