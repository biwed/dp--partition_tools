FROM apache/airflow:2.3.4

# Set project name argument
# Example: PROJECT=mymod

# Become root to install requirements
USER root
RUN apt-get update && apt-get install -y libpq-dev python-dev
ADD --chown=airflow:airflow yoyo /yoyo

ADD --chown=airflow:airflow requirements.txt requirements.txt

USER airflow
RUN pip install -r requirements.txt --no-cache-dir
RUN chmod -R 777 /yoyo
