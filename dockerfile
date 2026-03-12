FROM apache/airflow:2.9.0
USER root

# Install Java (needed for PySpark)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Upgrade pip and install dependencies
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir faker==40.8.0 pyspark==3.5.1

# Verify PySpark installation
RUN python -c "import pyspark; print('PySpark version:', pyspark.__version__)"
USER airflow
RUN pip uninstall -y apache-airflow-providers-openlineage
FROM apache/airflow:2.9.0

# Create the data lake directories
RUN mkdir -p /opt/airflow/data_lake/processed \
    && mkdir -p /opt/airflow/data_lake/raw

# Adjust ownership to the existing airflow user/group
RUN chown -R airflow:airflow /opt/airflow/data_lake \
    && chmod -R 775 /opt/airflow/data_lake

# Switch to airflow user for runtime
USER airflow
