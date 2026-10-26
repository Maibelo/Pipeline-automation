import os
import time
import logging
import schedule
import pandas as pd
from datetime import datetime

# Configure logging for production reliability
logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def extract() -> pd.DataFrame:
    """Simulate extracting data from a source (e.g., CSV, API, or Database)."""
    logging.info("Extraction started.")
    # Replace with real API calls or DB connections
    raw_data = {
        "timestamp": [datetime.now()],
        "product_id": [111],
        "price": ["R2000,00"],
        "quantity": [6]
    }
    return pd.DataFrame(raw_data)

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and process raw data."""
    logging.info("Transformation started.")
    df_clean = df.copy()
    # Clean string price into a numeric float
    df_clean["price"] = df_clean["price"].str.replace("$", "").astype(float)
    # Derive a new calculated feature
    df_clean["total_revenue"] = df_clean["price"] * df_clean["quantity"]
    return df_clean

def load(df: pd.DataFrame, target_path: str = "processed_data.csv"):
    """Load data to a final destination."""
    logging.info(f"Loading data to {target_path}.")
    # Append data if the file already exists, otherwise write header
    if not os.path.isfile(target_path):
        df.to_csv(target_path, index=False)
    else:
        df.to_csv(target_path, mode='a', header=False, index=False)

def run_pipeline():
    """Main ETL orchestration job execution."""
    logging.info("Pipeline execution triggered.")
    try:
        raw_df = extract()
        processed_df = transform(raw_df)
        load(processed_df)
        logging.info("Pipeline executed successfully.")
        print(f"[{datetime.now()}] Pipeline job finished successfully.")
    except Exception as e:
        logging.error(f"Pipeline failed with error: {e}")

# --- Automation Settings ---
# Schedule the pipeline job to run automatically every hour
schedule.every(1).hours.do(run_pipeline)

if __name__ == "__main__":
    print("Pipeline scheduler is running active in background...")
    logging.info("Pipeline scheduler initialized.")
    # Keep the script running to listen for scheduled intervals
    while True:
        schedule.run_pending()
        time.sleep(1)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments applied to all tasks inside the DAG
default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Define processing functions for individual DAG steps
def extract_task_callable(**kwargs):
    print("Extracting data from source systems...")
    # Return data or push metadata identifiers via XComs
    return {"status": "success", "extracted_rows": 1250}

def transform_task_callable(**kwargs):
    print("Transforming, cleaning, and validating records...")
    # Pull data upstream if required
    ti = kwargs['ti']
    extraction_meta = ti.xcom_pull(task_ids='extract_raw_data')
    print(f"Processing context metadata: {extraction_meta}")

def load_task_callable(**kwargs):
    print("Loading data into target Data Warehouse warehouse...")

# Initialize the DAG definition container
with DAG(
    dag_id="automated_data_pipeline_v1",
    default_args=default_args,
    description="An production automated ETL pipeline for core business metrics",
    schedule_interval="0 6 * * *",  # Runs daily at 06:00 AM UTC via Cron syntax
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["production", "etl"],
) as dag:

    # Step 1: Extraction
    extract_task = PythonOperator(
        task_id="extract_raw_data",
        python_callable=extract_task_callable,
    )

    # Step 2: Transformation
    transform_task = PythonOperator(
        task_id="transform_raw_data",
        python_callable=transform_task_callable,
    )

    # Step 3: Loading
    load_task = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_task_callable,
    )

    # Establish execution dependencies sequentially (Extract -> Transform -> Load)
    extract_task >> transform_task >> load_task

print('task running')