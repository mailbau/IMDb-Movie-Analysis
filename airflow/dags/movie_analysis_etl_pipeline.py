
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from etl.extract_movie_data import extract_movie_data
from etl.extract_imdb_reviews import extract_imdb_reviews
from etl.transform_date_format import transform_date_format
from etl.transform_calculate_weighted_rating import transform_calculate_weighted_rating
from etl.transform_sentiment_analysis import transform_sentiment_analysis
from etl.transform_top_keywords import transform_top_keywords
from etl.transform_join import transform_join
from etl.load_to_postgres import load_to_postgres

# Define default_args, schedule_interval, and DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'imdb_movie_analysis_etl',
    default_args=default_args,
    description='IMDb Movie Analysis ETL pipeline with Airflow',
    schedule_interval=timedelta(days=1),  # Adjust as needed
)

# Task 1: Extract Movie Data
extract_movie_data_task = PythonOperator(
    task_id='extract_movie_data',
    python_callable=extract_movie_data,
    dag=dag,
)

# Task 2: Extract User Reviews
extract_user_reviews_task = PythonOperator(
    task_id='extract_user_reviews',
    python_callable=extract_imdb_reviews,
    dag=dag,
)

# Task 3: Transform Date Format
transform_date_format_task = PythonOperator(
    task_id='transform_date_format',
    python_callable=transform_date_format,
    dag=dag,
)

# Task 4: Transform Calculate Weighted Rating
transform_calculate_weighted_rating_task = PythonOperator(
    task_id='transform_calculate_weighted_rating',
    python_callable=transform_calculate_weighted_rating,
    dag=dag,
)

# Task 5: Transform Sentiment Analysis
transform_sentiment_analysis_task = PythonOperator(
    task_id='transform_sentiment_analysis',
    python_callable=transform_sentiment_analysis,
    dag=dag,
)

# Task 6: Transform Top Keywords
transform_top_keywords_task = PythonOperator(
    task_id='transform_top_keywords',
    python_callable=transform_top_keywords,
    dag=dag,
)

# Task 7: Transform Join
transform_join_task = PythonOperator(
    task_id='transform_join',
    python_callable=transform_join,
    dag=dag,
)

# Task 8: Load to PostgreSQL
load_to_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

# Set task dependencies
extract_movie_data_task >> extract_user_reviews_task
extract_user_reviews_task >> transform_date_format_task
transform_date_format_task >> transform_calculate_weighted_rating_task
transform_calculate_weighted_rating_task >> transform_sentiment_analysis_task
transform_sentiment_analysis_task >> transform_top_keywords_task
transform_top_keywords_task >> transform_join_task
transform_join_task >> load_to_postgres_task
