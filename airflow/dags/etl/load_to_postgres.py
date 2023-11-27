import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def load_to_postgres():
    # Get the absolute path of the current script
    current_directory = os.path.dirname(os.path.abspath(__file__))
    # Load the combined_data CSV into a pandas DataFrame
    csv_path = os.path.join(current_directory, 'combined_data.csv')
    df_combined = pd.read_csv(csv_path)

    # Connect to PostgreSQL
    engine = create_engine('postgresql://postgres:ilovejunkfood321@host.docker.internal:5432/RekDat_IMDb')

    # Write the DataFrame to PostgreSQL
    df_combined.to_sql('movie_data', engine, if_exists='replace', index=False)

if __name__ == '__main__':
    load_to_postgres()