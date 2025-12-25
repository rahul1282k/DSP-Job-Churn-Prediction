FROM apache/airflow:2.9.3-python3.10

# Switch to root to install OS packages and fix permissions
USER root

RUN apt-get update --yes \
    && apt-get install --yes postgresql-client netcat-openbsd \
    && apt-get clean

# Copy entrypoint and make it executable as root
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Copy requirements
COPY requirements.txt /opt/airflow/requirements.txt

# Install Python dependencies as airflow user (recommended)
USER airflow
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Switch back to root to set entrypoint
USER root
ENTRYPOINT ["/entrypoint.sh"]
