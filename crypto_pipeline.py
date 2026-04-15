# this file saved in Users/faijanahamed/airflow/dags
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import os
import subprocess
import boto3
from botocore.config import Config
import snowflake.connector
import time
from datetime import timedelta


# ================= CONFIG =================
LOCAL_FILE = "/Users/faijanahamed/Desktop/Data Engineering/DBT/crypto_pipeline/crypto_project/data/crypto_data.csv"

S3_BUCKET = "cryptobucket2"
S3_KEY = "crypto/crypto_data.csv"

AWS_KEY = "airflow-user_aws_key"
AWS_SECRET = "airflow-user_aws_secret"


# ================= STEP 1 =================
def fetch_crypto_data():
    print("🔥 FUNCTION STARTED")

    base_path = "/Users/faijanahamed/Desktop/Data Engineering/DBT/crypto_pipeline/crypto_project/data"
    file_path = os.path.join(base_path, "crypto_data.csv")
    os.makedirs(base_path, exist_ok=True)

    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "per_page": 200
    }
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    # Retry config
    MAX_RETRIES = 5
    BACKOFF_SECONDS = [10, 30, 60, 120, 180]  # wait time per retry attempt

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Calling API... (Attempt {attempt}/{MAX_RETRIES})")

            response = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=(10, 30)   # increased timeouts
            )

            print(f"Status Code: {response.status_code}")

            # Handle rate limiting explicitly
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", BACKOFF_SECONDS[attempt - 1]))
                print(f"⚠️ Rate limited! Waiting {retry_after}s before retry...")
                time.sleep(retry_after)
                continue

            # Handle server errors (5xx) — retry
            if response.status_code in [500, 502, 503, 504]:
                wait = BACKOFF_SECONDS[attempt - 1]
                print(f"⚠️ Server error {response.status_code}. Retrying in {wait}s...")
                time.sleep(wait)
                continue

            # Any other non-200 — fail immediately, no retry
            if response.status_code != 200:
                raise Exception(f"❌ API failed with status: {response.status_code} | Body: {response.text[:200]}")

            data = response.json()

            # Validate response is not empty
            if not data:
                raise Exception("❌ API returned empty data")

            df = pd.DataFrame(data)[[
                "id",
                "symbol",
                "name",
                "current_price",
                "market_cap"
            ]]

            # Validate dataframe before saving
            if df.empty:
                raise Exception("❌ DataFrame is empty after parsing")

            df.to_csv(file_path, index=False)
            print(f"CSV Saved Successfully! Rows: {len(df)}")
            return

        except requests.exceptions.ConnectionError as e:
            wait = BACKOFF_SECONDS[attempt - 1]
            print(f"❌ Connection error on attempt {attempt}: {e}. Retrying in {wait}s...")
            time.sleep(wait)

        except requests.exceptions.Timeout as e:
            wait = BACKOFF_SECONDS[attempt - 1]
            print(f"Timeout on attempt {attempt}. Retrying in {wait}s...")
            time.sleep(wait)

        except requests.exceptions.RequestException as e:
            wait = BACKOFF_SECONDS[attempt - 1]
            print(f"❌ Request error on attempt {attempt}: {e}. Retrying in {wait}s...")
            time.sleep(wait)

    # All retries exhausted
    raise Exception(f"❌ API failed after {MAX_RETRIES} attempts. Pipeline stopped.")


# ================= STEP 2 =================
import boto3
from botocore.config import Config
import os
import time

def upload_to_s3():
    print("Uploading to S3...")

    MAX_RETRIES = 3
    BACKOFF = [10, 30, 60]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"Attempt {attempt}/{MAX_RETRIES}")

            # Fix 1: Add timeouts to boto3 — default is NO timeout (hangs forever)
            boto_config = Config(
                connect_timeout=10,
                read_timeout=30,
                retries={"max_attempts": 3, "mode": "adaptive"}
            )

            print("🔧 Creating S3 client...")
            s3 = boto3.client(
                "s3",
                aws_access_key_id=AWS_KEY,
                aws_secret_access_key=AWS_SECRET,
                region_name="eu-north-1",
                config=boto_config     
            )
            print("S3 client created")

            # Fix 2: Check file exists before attempting upload
            print("File exists:", os.path.exists(LOCAL_FILE))
            if not os.path.exists(LOCAL_FILE):
                raise FileNotFoundError(f"❌ File not found: {LOCAL_FILE}")

            file_size = os.path.getsize(LOCAL_FILE)
            print(f"File size: {file_size} bytes")

            # Fix 3: Use upload_file instead of put_object (handles large files + progress)
            s3.upload_file(
                Filename=LOCAL_FILE,
                Bucket=S3_BUCKET,
                Key=S3_KEY,
                Callback=UploadProgressLogger(file_size)
            )

            print("Uploaded to S3 successfully!")
            return 

        except FileNotFoundError as e:
            print(f"❌ {e}")
            raise  # no point retrying if file doesn't exist

        except s3.exceptions.NoCredentialsError:
            print("❌ AWS credentials invalid — check AWS_KEY and AWS_SECRET")
            raise  # no point retrying bad credentials

        except Exception as e:
            wait = BACKOFF[attempt - 1]
            print(f"❌ Upload error on attempt {attempt}: {str(e)}")
            if attempt < MAX_RETRIES:
                print(f"⏳ Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print("❌ All retries exhausted.")
                raise


# Fix 4: Progress logger so you can see upload isn't frozen
class UploadProgressLogger:
    def __init__(self, total_size):
        self.total = total_size
        self.uploaded = 0

    def __call__(self, bytes_transferred):
        self.uploaded += bytes_transferred
        pct = (self.uploaded / self.total) * 100 if self.total > 0 else 0
        print(f"📤 Uploaded: {self.uploaded}/{self.total} bytes ({pct:.1f}%)")



# ================= STEP 3 =================
def load_from_s3():
    print("Loading from S3 to Snowflake...")

    conn = None
    cs = None

    try:
        conn = snowflake.connector.connect(
            user="FAIJANAHAMED6",
            password="my_snowflake_password",
            account="MGEFSZM-YHB72467",
            warehouse="COMPUTE_WH",
            database="CRYPTO_DB",
            schema="RAW",
            login_timeout=30,
            network_timeout=60,
        )

        cs = conn.cursor()
        cs.execute("BEGIN")
        cs.execute("TRUNCATE TABLE crypto_data")

        # Use FILES = instead of appending path
        cs.execute("""
            COPY INTO crypto_data
            FROM @my_s3_stage
            FILES=('crypto_data.csv')
            FILE_FORMAT = my_csv_format
            ON_ERROR = 'CONTINUE'
        """)

        # results = cs.fetchall()
        # print(f"COPY INTO result: {results}")

        conn.commit()
        print("Snowflake loaded successfully!")

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        if conn:
            conn.rollback()
        raise

    finally:
        if cs:
            cs.close()
        if conn:
            conn.close()

# ================= STEP 4 =================
def run_dbt():
    print("Running dbt...")

    result = subprocess.run(
        ["/Users/faijanahamed/Desktop/Data Engineering/DBT/crypto_pipeline/venv/bin/dbt", "run"],
        cwd="/Users/faijanahamed/Desktop/Data Engineering/DBT/crypto_pipeline/crypto_project",
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        raise Exception("dbt failed")


# ================= DAG =================
dag = DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False
)

fetch_task = PythonOperator(
    task_id="fetch",
    python_callable=fetch_crypto_data,
    retries=3,                              # Airflow retries the whole task
    retry_exponential_backoff=True,         # doubles wait time each retry
    dag=dag
)

upload_task = PythonOperator(
    task_id="upload_s3",
    python_callable=upload_to_s3,
    retries=3,
    retry_delay=timedelta(minutes=2),
    execution_timeout=timedelta(minutes=10),  # kills task if frozen > 10 min
    dag=dag
)

load_task = PythonOperator(
    task_id="load_snowflake",
    python_callable=load_from_s3,
    dag=dag
)

dbt_task = PythonOperator(
    task_id="run_dbt",
    python_callable=run_dbt,
    dag=dag
)

fetch_task >> upload_task >> load_task >> dbt_task