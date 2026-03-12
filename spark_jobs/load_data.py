import os
import shutil
import logging
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, row_number, lit
from pyspark.sql.window import Window
import psycopg2

# ------------------------------
# Logging configuration
# ------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ------------------------------
# Paths
# ------------------------------
PROCESSED_PARQUET = "/opt/airflow/data_lake/processed/finance_processed.parquet"
CURATED_DIR = "/opt/airflow/data_lake/curated"
os.makedirs(CURATED_DIR, exist_ok=True)

# ------------------------------
# Postgres configuration
# ------------------------------
DB_HOST = "host.docker.internal"
DB_NAME = "finance_dw"
DB_USER = "finance_user"
DB_PASSWORD = "passwordasyouwant"

jdbc_url = f"jdbc:postgresql://{DB_HOST}:5432/{DB_NAME}"

properties = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# ------------------------------
# Wait for PostgreSQL
# ------------------------------
def wait_for_postgres(retries=5, delay=5):
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            conn.close()
            logger.info("PostgreSQL connection successful.")
            return True
        except Exception as e:
            logger.warning(f"Postgres not reachable ({i+1}/{retries}). Waiting {delay}s... {e}")
            time.sleep(delay)

    logger.error("Postgres not reachable after retries.")
    return False

# ------------------------------
# Safe parquet writer
# ------------------------------
def safe_write(df, path):
    if os.path.exists(path):
        try:
            shutil.rmtree(path)
        except PermissionError:
            logger.warning(f"Permission denied when removing {path}. Attempting with forced removal...")
            os.system(f"rm -rf {path}")
    df.write.mode("overwrite").parquet(path)

# ------------------------------
# Check if table empty
# ------------------------------
def is_table_empty(table_name):
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    # Fix: properly quote schema and table name separately
    schema, table = table_name.split('.')
    cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count == 0

# ------------------------------
# Truncate table instead of drop
# ------------------------------
def truncate_table(table_name):
    conn = psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    # Fix: properly quote schema and table name separately
    schema, table = table_name.split('.')
    cur.execute(f'TRUNCATE TABLE "{schema}"."{table}" RESTART IDENTITY CASCADE;')
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Table {table_name} truncated successfully.")

# ------------------------------
# Load dataframe into postgres
# ------------------------------
def load_to_postgres(df, table_name):
    rows = df.count()
    if rows == 0:
        logger.info(f"No records for {table_name}")
        return

    # Always truncate before loading to avoid duplicate key conflicts
    truncate_table(table_name)
    logger.info(f"{table_name} truncated → full reload ({rows})")

    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    logger.info(f"{table_name} loaded successfully")

# ------------------------------
# Main load
# ------------------------------
def load_curated_layer():
    logger.info("Starting curated load")

    if not wait_for_postgres():
        return

    spark = SparkSession.builder \
        .appName("FinanceLoad") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    # ------------------------------
    # Read processed parquet
    # ------------------------------
    if not os.path.exists(PROCESSED_PARQUET):
        logger.warning("Processed parquet not found")
        spark.stop()
        return

    logger.info("Reading processed parquet")
    df = spark.read.parquet(PROCESSED_PARQUET)
    record_count = df.count()
    logger.info(f"Records found: {record_count}")

    if record_count == 0:
        logger.warning("No records in processed data")
        spark.stop()
        return

    df = df.withColumn("load_timestamp", lit(datetime.utcnow()))

    # ------------------------------
    # DIM CUSTOMER
    # Drop duplicates prioritizing email (use first occurrence by name)
    window_customer = Window.partitionBy("customer_email").orderBy("customer_name")
    dim_customer = df.select("customer_name", "customer_email") \
                     .withColumn("row_num", row_number().over(window_customer)) \
                     .filter("row_num = 1") \
                     .drop("row_num") \
                     .withColumn("customer_id", row_number().over(Window.orderBy("customer_email"))) \
                     .repartition(1)  # Ensure sequential write to avoid constraints

    safe_write(dim_customer, f"{CURATED_DIR}/dim_customer.parquet")
    load_to_postgres(dim_customer, "finance.dim_customer")

    # ------------------------------
    # DIM MERCHANT
    # Drop duplicates on merchant_name
    window_merchant = Window.orderBy("merchant_name")
    dim_merchant = df.select("merchant_name") \
                     .dropDuplicates() \
                     .withColumn("merchant_id", row_number().over(window_merchant)) \
                     .repartition(1)  # Ensure sequential write

    safe_write(dim_merchant, f"{CURATED_DIR}/dim_merchant.parquet")
    load_to_postgres(dim_merchant, "finance.dim_merchant")

    # ------------------------------
    # DIM DATE
    # Generate date dimension with proper deduplication
    window_date = Window.orderBy("transaction_date")
    dim_date = df.select("transaction_date") \
                 .dropDuplicates() \
                 .withColumn("date_id", row_number().over(window_date)) \
                 .withColumn("year", year("transaction_date")) \
                 .withColumn("month", month("transaction_date")) \
                 .withColumn("day", dayofmonth("transaction_date")) \
                 .withColumn("weekday", dayofweek("transaction_date")) \
                 .repartition(1)  # Ensure sequential write

    safe_write(dim_date, f"{CURATED_DIR}/dim_date.parquet")
    load_to_postgres(dim_date, "finance.dim_date")

    # ------------------------------
    # FACT TABLE
    # ------------------------------
    fact_transactions = df \
        .join(dim_customer, ["customer_name", "customer_email"], "left") \
        .join(dim_merchant, ["merchant_name"], "left") \
        .join(dim_date, ["transaction_date"], "left") \
        .select(
            "transaction_id",
            "customer_id",
            "merchant_id",
            "transaction_amount",
            "date_id",
            "load_timestamp"
        )

    safe_write(fact_transactions, f"{CURATED_DIR}/fact_transactions.parquet")
    load_to_postgres(fact_transactions, "finance.fact_transactions")

    spark.stop()
    logger.info("Curated load finished successfully")

# ------------------------------
# Entry point
# ------------------------------
if __name__ == "__main__":
    load_curated_layer()