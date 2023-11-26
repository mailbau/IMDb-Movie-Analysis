import pandas as pd

df = pd.read_csv('etl/movie_data_transformed.csv')

# Set a threshold for the minimum number of votes
min_votes = 1000

# Calculate the weighted rating
df['weighted_rating'] = (df['vote_count'] / (df['vote_count'] + min_votes)) * df['vote_average'] + (min_votes / (df['vote_count'] + min_votes)) * df['vote_average'].mean()

# Save the updated DataFrame back to a CSV file
df.to_csv('etl/movie_data_transformed.csv', index=False)
