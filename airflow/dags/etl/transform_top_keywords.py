import os
import pandas as pd
import nltk
nltk.download('punkt')
nltk.download('stopwords')
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from collections import Counter

def transform_top_keywords():
    # Get the absolute path of the current script
    current_directory = os.path.dirname(os.path.abspath(__file__))
    # Load the CSV into a pandas DataFrame
    csv_path = os.path.join(current_directory, 'user_reviews_transformed.csv')
    df_reviews = pd.read_csv(csv_path)

    # Combine all reviews into a single text
    all_reviews_text = ' '.join(df_reviews['user_reviews'].astype(str).values)

    # Tokenize the text into words
    tokens = word_tokenize(all_reviews_text)

    # Remove stopwords and specific words to exclude
    stop_words = set(stopwords.words('english'))
    words_to_exclude = set(['film', 'movie', 'first', 'one'])  # Add any additional words to exclude
    filtered_tokens = [word.lower() for word in tokens if word.isalpha() and word.lower() not in stop_words and word.lower() not in words_to_exclude]

    # Count the frequency of each word
    word_counts = Counter(filtered_tokens)

    # Get the top N keywords
    top_keywords = dict(word_counts.most_common(10))

    # Add a new column 'top_keywords' to the DataFrame
    df_reviews['top_keywords'] = df_reviews['user_reviews'].apply(lambda x: [word for word in word_tokenize(x) if word.lower() in top_keywords])

    # Save the updated DataFrame back to a CSV file
    transformed_csv_path = os.path.join(current_directory, 'user_reviews_transformed.csv')
    df_reviews.to_csv(transformed_csv_path, index=False)

if __name__ == '__main__':
    transform_top_keywords()