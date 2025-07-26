# Dockerfile
ARG AIRFLOW_VERSION=2.9.3
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY airflow.requirements.txt .
RUN pip install --no-cache-dir -r airflow.requirements.txt