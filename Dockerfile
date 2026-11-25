FROM apache/airflow:2.10.5-python3.11
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
USER airflow
RUN mkdir -p /opt/airflow/work
RUN pip install --no-cache-dir "apache-airflow==2.10.5" pyspark==3.5.5 boto3==1.37.38 requests==2.32.3 confluent-kafka==2.15.0
