from curses import raw
from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pandas as pd

from src.extract.generator import generate_fake_users
from src.transform.clean_data import transform_user_data
from src.load.postgres_loader import load_to_postgres

default_args = {
    'owner': 'data_engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,
    'email_on_retry': False,
}

@dag(
    dag_id='faker_to_postgres_etl',
    default_args=default_args,
    description='Simple ETL pipeline using Faker and PostgreSQL',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'faker', 'postgres'],
)
def etl_pipeline():

    @task()
    def extract_task():
        """Generates raw user data."""
        df = generate_fake_users(num_records=100)
        return df.to_dict(orient='records')  # Serialize DataFrame for Airflow XCom

    @task()
    def transform_task(raw_data):
        """Cleans and transforms raw data."""
        raw_df = pd.DataFrame(raw_data)
        clean_df = transform_user_data(raw_df)

        # Convert Timestamp columns to string format for Airflow JSON XCom
        if "signup_date" in clean_df.columns:
            clean_df["signup_date"] = clean_df["signup_date"].dt.strftime(
            "%Y-%m-%d"
        )
        return clean_df.to_dict(orient='records')

    @task()
    def load_task(clean_data):
        """Loads cleaned data into PostgreSQL."""
        clean_df = pd.DataFrame(clean_data)

        load_to_postgres(clean_df, table_name="dim_users")

    # Define task dependencies (Execution Flow)
    raw_data = extract_task()
    clean_data = transform_task(raw_data)
    load_task(clean_data)


# Instantiate the DAG
etl_dag = etl_pipeline()