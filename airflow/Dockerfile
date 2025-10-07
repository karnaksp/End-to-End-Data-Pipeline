FROM apache/airflow:2.8.0-python3.9

USER root
RUN apt-get update && apt-get install -y gcc libpq-dev netcat-traditional tzdata
RUN mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/plugins && \
    chown -R airflow:root /opt/airflow && \
    chmod -R 755 /opt/airflow

COPY init.sh /init.sh
RUN chmod +x /init.sh

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Переменные окружения для конфигурации
ENV AIRFLOW__CORE__DEFAULT_TIMEZONE=Europe/Moscow

ENTRYPOINT ["/init.sh"]
# CMD ["webserver"]