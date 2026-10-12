FROM apache/airflow:3.1.0

USER root

RUN apt-get update && \
    apt-get install -y git bash curl unzip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# requirements.txt faylını konteynerə köçür və quraşdır
COPY requirements.txt /opt/airflow/
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt