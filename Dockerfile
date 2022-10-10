FROM apache/airflow:2.3.4

# Set project name argument
# Example: PROJECT=mymod
ARG AIRFLOW_PROJECT=partition_tools

# Become root to install requirements
USER root
ADD --chown=airflow:airflow yoyo /yoyo

ADD requirements.txt requirements.txt

RUN pip install -r requirements.txt \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base \
        ~/.cache/pip

# Switch back to airflow user
USER airflow

ADD dags /opt/airflow/dags/${AIRFLOW_PROJECT}
