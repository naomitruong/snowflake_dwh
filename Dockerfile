FROM apache/airflow:3.3.2
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.13.txt"
COPY dbt/requirements.txt /dbt-requirements.txt
RUN python -m venv /opt/airflow/dbt-venv && \
    /opt/airflow/dbt-venv/bin/pip install --no-cache-dir -r /dbt-requirements.txt
