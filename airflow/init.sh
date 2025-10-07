#!/bin/bash

echo "Received arguments: $@"
echo "First argument: $1"

# Wait for PostgreSQL to be ready
while ! nc -z postgres 5432; do
  echo "Waiting for PostgreSQL to start..."
  sleep 1
done

# Initialize the database only if not initialized yet
if [ ! -f "$AIRFLOW_HOME/.db_initialized" ]; then
    echo "Initializing Airflow database..."
    airflow db init
    touch "$AIRFLOW_HOME/.db_initialized"
fi

# Create admin user if it doesn't exist
echo "Checking for admin user..."
airflow users list | grep -q "${AIRFLOW_ADMIN_USER:-admin}" || \
airflow users create \
    --username "${AIRFLOW_ADMIN_USER:-admin}" \
    --firstname "${AIRFLOW_ADMIN_FIRSTNAME:-admin}" \
    --lastname "${AIRFLOW_ADMIN_LASTNAME:-admin}" \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL:-admin@example.com}" \
    --password "${AIRFLOW_ADMIN_PASSWORD:-admin}"

# Check if we should start the webserver or scheduler based on the command
echo "Starting with command: $1"
if [ "$1" = "webserver" ]; then
    echo "Starting Airflow webserver..."
    exec airflow webserver
elif [ "$1" = "scheduler" ]; then
    echo "Starting Airflow scheduler..."
    exec airflow scheduler
else
    echo "No valid command provided, defaulting to webserver..."
    exec airflow webserver
fi