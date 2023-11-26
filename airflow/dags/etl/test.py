import pandas as pd

df = pd.read_csv('etl/user_reviews_transformed.csv')

print(df['top_keywords'])