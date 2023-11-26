import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Load the combined data from CSV
df_combined = pd.read_csv('etl/combined_data.csv')

# Connect to PostgreSQL
engine = create_engine('postgresql://postgres:ilovejunkfood321@localhost:5432/RekDat_IMDb')

# Write the DataFrame to PostgreSQL
df_combined.to_sql('movie_data', engine, if_exists='replace', index=False)
