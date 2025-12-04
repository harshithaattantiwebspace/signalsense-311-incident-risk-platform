FROM apache/airflow:2.9.2-python3.11

# Run all commands as the 'airflow' user (NOT root)
USER airflow

# Install dbt + BigQuery client inside the image as airflow
RUN pip install --no-cache-dir \
    "click<8.2" \
    "dbt-core==1.9.0" \
    "dbt-bigquery==1.9.0" \
    "google-cloud-bigquery"
