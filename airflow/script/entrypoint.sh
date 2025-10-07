#!/bin/bash

# Wait for PostgreSQL
airflow db check

# Initialize the database if needed
airflow db init

# Create admin user if it doesn't exist
airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin || true

# Start the webserver
exec airflow webserver
