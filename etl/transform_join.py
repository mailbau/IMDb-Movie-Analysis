import pandas as pd

# Load the transformed movie data
df_movies = pd.read_csv('etl/movie_data_transformed.csv')

# Load the transformed user reviews data
df_reviews = pd.read_csv('etl/user_reviews_transformed.csv')

# Merge the datasets on the common column 'imdb_id'
df_combined = pd.merge(df_movies, df_reviews, on='imdb_id', how='inner')

# Save the combined DataFrame to a new CSV file
df_combined.to_csv('etl/combined_data.csv', index=False)
