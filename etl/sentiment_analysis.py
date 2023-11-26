import pandas as pd
import nltk
nltk.download('vader_lexicon')
from nltk.sentiment import SentimentIntensityAnalyzer

# Load the CSV into a pandas DataFrame
df_reviews = pd.read_csv('raw_data/user_reviews.csv')

# Initialize the Sentiment Intensity Analyzer
sia = SentimentIntensityAnalyzer()

# Apply sentiment analysis to each review and create a new column 'sentiment'
df_reviews['sentiment'] = df_reviews['user_reviews'].apply(lambda x: sia.polarity_scores(x)['compound'])

# Save the updated DataFrame back to a CSV file
df_reviews.to_csv('etl/user_reviews_transformed.csv', index=False)
