import os
import pandas as pd
import nltk
nltk.download('vader_lexicon')
from nltk.sentiment import SentimentIntensityAnalyzer

def transform_sentiment_analysis():
    # Get the absolute path of the current script
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Load the CSV into a pandas DataFrame
    csv_path = os.path.join(current_directory, 'raw_data', 'user_reviews.csv')
    df_reviews = pd.read_csv(csv_path)

    # Initialize the Sentiment Intensity Analyzer
    sia = SentimentIntensityAnalyzer()

    # Apply sentiment analysis to each review and create a new column 'sentiment'
    df_reviews['sentiment'] = df_reviews['user_reviews'].apply(lambda x: sia.polarity_scores(x)['compound'])

    # Save the updated DataFrame back to a CSV file
    transformed_csv_path = os.path.join(current_directory, 'user_reviews_transformed.csv')
    df_reviews.to_csv(transformed_csv_path, index=False)

if __name__ == '__main__':
    transform_sentiment_analysis()