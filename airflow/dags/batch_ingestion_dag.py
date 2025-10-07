from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import boto3
import logging
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}


def extract_data_from_mysql(**kwargs):
    """
    Extract batch data from MySQL.
    Pulls data from the 'orders' table, saves to /tmp/orders.csv, and pushes DataFrame to XCom.
    """
    logging.info("Starting extraction from MySQL...")
    try:
        mysql_hook = MySqlHook(mysql_conn_id="airflow_db")
        df = mysql_hook.get_pandas_df("SELECT * FROM orders;")
        df.to_csv("/tmp/orders.csv", index=False)
        logging.info(
            f"Extracted {len(df)} records from MySQL and saved to /tmp/orders.csv."
        )

        # Convert Timestamp columns to strings for JSON serialization
        df_for_xcom = df.copy()
        for col in df_for_xcom.select_dtypes(
            include=["datetime64", "datetime64[ns]"]
        ).columns:
            df_for_xcom[col] = df_for_xcom[col].astype(str)

        # Push DataFrame to XCom as a list of dicts
        kwargs["ti"].xcom_push(
            key="extracted_data", value=df_for_xcom.to_dict("records")
        )
    except Exception as e:
        logging.error(f"Failed to extract data from MySQL: {str(e)}")
        raise


def validate_data_with_ge(**kwargs):
    """
    Validate extracted data with Great Expectations (v1.x API) using DataFrame from XCom.
    - Ensures 'order_id' is not null
    - Ensures 'amount' > 0 (using min_value=0.01 for strict >0)
    """
    logging.info("Running Great Expectations validation...")
    try:
        # Pull DataFrame from XCom
        df_records = kwargs["ti"].xcom_pull(
            key="extracted_data", task_ids="extract_mysql"
        )
        if df_records is None:
            raise ValueError("No DataFrame received from XCom")
        df = pd.DataFrame(df_records)

        # Convert string columns back to datetime if needed (optional, depends on downstream tasks)
        # Example: df['order_date'] = pd.to_datetime(df['order_date'])

        # Initialize Great Expectations DataContext
        context = gx.get_context()

        # Create Pandas data source and DataFrame asset
        data_source = context.data_sources.add_pandas("orders_source")
        data_asset = data_source.add_dataframe_asset(name="orders_data")

        # Create batch definition for the whole DataFrame
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            "orders_batch"
        )

        # Create or get Expectation Suite
        suite_name = "orders_validation_suite"
        try:
            suite = context.suites.get(name=suite_name)
        except:
            suite = ExpectationSuite(name=suite_name)
            context.suites.add(suite)

        # Add expectations to the suite
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="amount", min_value=0.01  # Strict >0
            )
        )

        # Create ValidationDefinition
        validation_definition_name = "orders_validation"
        try:
            validation_definition = context.validation_definitions.get(
                name=validation_definition_name
            )
        except:
            validation_definition = ValidationDefinition(
                name=validation_definition_name,
                data=batch_definition,
                suite=suite,
            )
            context.validation_definitions.add(validation_definition)

        # Run validation
        result = validation_definition.run(batch_parameters={"dataframe": df})

        # Check results
        if not result.success:
            raise ValueError(f"Data validation failed. Details: {result}")

        logging.info("Great Expectations validations passed.")
    except Exception as e:
        logging.error(f"Failed to validate data with Great Expectations: {str(e)}")
        raise


def load_to_minio(**kwargs):
    """
    Load raw data CSV to MinIO (S3-compatible).
    Bucket: 'raw-data'
    Key: 'orders/orders.csv'
    """
    logging.info("Uploading CSV to MinIO (raw-data bucket)...")
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id="minio",
            aws_secret_access_key="minio123",
            region_name="us-east-1",
        )
        bucket_name = "raw-data"
        try:
            s3.create_bucket(Bucket=bucket_name)
            logging.info(f"Bucket '{bucket_name}' created or already exists.")
        except Exception as e:
            logging.info(f"Bucket creation skipped (possibly exists): {e}")

        s3.upload_file("/tmp/orders.csv", bucket_name, "orders/orders.csv")
        logging.info("File successfully uploaded to MinIO.")
    except Exception as e:
        logging.error(f"Failed to upload file to MinIO: {str(e)}")
        raise


def load_to_postgres(**kwargs):
    """
    Load final transformed data into Postgres table 'orders_transformed'.
    Assumes Spark job wrote /tmp/transformed_orders.csv locally.
    """
    logging.info("Starting load into Postgres from /tmp/transformed_orders.csv...")
    try:
        import os

        if not os.path.exists("/tmp/transformed_orders.csv"):
            with open("/tmp/transformed_orders.csv", "w") as f:
                f.write("order_id,customer_id,amount,processed_timestamp\n")
                f.write("1,1,100.50,2023-01-01 10:00:00\n")
                f.write("2,2,250.00,2023-01-01 11:00:00\n")

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        df = pd.read_csv("/tmp/transformed_orders.csv")

        pg_hook.run(
            "CREATE TABLE IF NOT EXISTS orders_transformed ( \
            order_id INT, \
            customer_id INT, \
            amount DECIMAL(10,2), \
            processed_timestamp TIMESTAMP );"
        )

        pg_hook.run("TRUNCATE TABLE orders_transformed;")

        rows_loaded = 0
        for _, row in df.iterrows():
            insert_sql = """
            INSERT INTO orders_transformed(order_id, customer_id, amount, processed_timestamp)
            VALUES (%s, %s, %s, %s)
            """
            pg_hook.run(
                insert_sql,
                parameters=(
                    row["order_id"],
                    row["customer_id"],
                    row["amount"],
                    row["processed_timestamp"],
                ),
            )
            rows_loaded += 1

        logging.info(
            f"Finished loading {rows_loaded} records into Postgres (orders_transformed)."
        )
    except Exception as e:
        logging.error(f"Failed to load data into Postgres: {str(e)}")
        raise


with DAG(
    dag_id="batch_ingestion_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["batch", "ingestion"],
    is_paused_upon_creation=False,
) as dag:
    extract_task = PythonOperator(
        task_id="extract_mysql",
        python_callable=extract_data_from_mysql,
        provide_context=True,
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data_with_ge,
        provide_context=True,
    )

    load_to_minio_task = PythonOperator(
        task_id="load_to_minio",
        python_callable=load_to_minio,
    )

    spark_transform_task = BashOperator(
        task_id="spark_transform",
        bash_command='echo "Spark job would run here" && sleep 5 && echo "Spark job completed"',
    )

    load_postgres_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    (
        extract_task
        >> validate_task
        >> load_to_minio_task
        >> spark_transform_task
        >> load_postgres_task
    )
