from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Define default_args, schedule_interval, and DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'imdb_movie_analysis_etl',
    default_args=default_args,
    description='IMDb Movie Analysis ETL pipeline with Airflow',
    schedule_interval=timedelta(days=1),  # Adjust as needed
)

# Task 1: Extract Movie Data
extract_movie_data_task = BashOperator(
    task_id='extract_movie_data',
    bash_command='python /etl/extract_movie_data.py',
    dag=dag,
)

# Task 2: Extract User Reviews
extract_user_reviews_task = BashOperator(
    task_id='extract_user_reviews',
    bash_command='python /etl/extract_user_reviews.py',
    dag=dag,
)

# Task 3: Transform Date Format
transform_date_format_task = BashOperator(
    task_id='transform_date_format',
    bash_command='python /etl/transform_date_format.py',
    dag=dag,
)

# Task 4: Transform Calculate Weighted Rating
transform_calculate_weighted_rating_task = BashOperator(
    task_id='transform_calculate_weighted_rating',
    bash_command='python /etl/transform_calculate_weighted_rating.py',
    dag=dag,
)

# Task 5: Transform Sentiment Analysis
transform_sentiment_analysis_task = BashOperator(
    task_id='transform_sentiment_analysis',
    bash_command='python /etl/transform_sentiment_analysis.py',
    dag=dag,
)

# Task 6: Transform Top Keywords
transform_top_keywords_task = BashOperator(
    task_id='transform_top_keywords',
    bash_command='python /etl/transform_top_keywords.py',
    dag=dag,
)

# Task 7: Transform Join
transform_join_task = BashOperator(
    task_id='transform_join',
    bash_command='python /etl/transform_join.py',
    dag=dag,
)

# Task 8: Load to PostgreSQL
load_to_postgres_task = BashOperator(
    task_id='load_to_postgres',
    bash_command='python /etl/load_to_postgres.py',
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
