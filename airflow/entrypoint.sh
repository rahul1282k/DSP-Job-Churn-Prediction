#!/bin/bash
set -e

echo "Waiting for postgres..."
while ! nc -z postgres 5432; do
  sleep 1
done
echo "Postgres is ready!"

echo "Initializing Airflow DB..."
airflow db migrate

echo "Creating Airflow admin user (if not exists)..."
airflow users create \
  --username admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com \
  --password admin || true

echo "Starting Airflow scheduler..."
airflow scheduler &

echo "Starting Airflow webserver..."
# Clean stale pid file to avoid webserver crash
rm -f /opt/airflow/airflow-webserver.pid

exec airflow webserver
