# Use the official Airflow image as the base image
FROM apache/airflow:2.5.1

# Switch to the root user to install additional packages
USER root

# Install NLTK and other necessary packages
RUN su airflow -c "pip install nltk"

# Switch back to the airflow user
USER airflow
