import pandas as pd

def transform_date_format():
    # Load the CSV into a pandas DataFrame
    df = pd.read_csv('raw_data/movie_data.csv')

    # Convert the 'release_date' column to datetime format
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

    # Convert the 'release_date' column to the standard YYYY-MM-DD format
    df['release_date'] = df['release_date'].dt.strftime('%Y-%m-%d')

    # Save the updated DataFrame back to a CSV file
    df.to_csv('etl/movie_data_transformed.csv', index=False)

if __name__ == '__main__':
    transform_date_format()
