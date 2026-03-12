import os
import logging
from pyspark.sql import SparkSession

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
SOURCE_FILE = "/opt/airflow/data_lake/raw/transaction_data.csv"
RAW_DIR = "/opt/airflow/data_lake/raw"
PROCESSED_DIR = "/opt/airflow/data_lake/processed"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

RAW_PARQUET = os.path.join(RAW_DIR, "finance_raw.parquet")
PROCESSED_PARQUET = os.path.join(PROCESSED_DIR, "finance_processed.parquet")

# ------------------------------
# Ingest Function
# ------------------------------
def ingest_data():

    logger.info("Starting ingestion process...")

    spark = SparkSession.builder \
        .appName("FinanceIngestion") \
        .getOrCreate()

    # ------------------------------
    # Check if source exists
    # ------------------------------
    if not os.path.exists(SOURCE_FILE):
        logger.error(f"Source file not found: {SOURCE_FILE}")
        spark.stop()
        return

    # ------------------------------
    # Read CSV
    # ------------------------------
    logger.info("Reading source CSV file...")
    
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(SOURCE_FILE)

    record_count = df.count()
    logger.info(f"Total records ingested: {record_count}")

    if record_count == 0:
        logger.warning("No records found in source file.")
        spark.stop()
        return

    # ------------------------------
    # Write Raw Layer
    # ------------------------------
    logger.info("Writing raw parquet...")
    
    df.write.mode("overwrite").parquet(RAW_PARQUET)

    # ------------------------------
    # Write Processed Layer
    # ------------------------------
    logger.info("Writing processed parquet...")

    df.write.mode("overwrite").parquet(PROCESSED_PARQUET)

    logger.info("Ingestion completed successfully.")

    spark.stop()

# ------------------------------
# Entry point
# ------------------------------
if __name__ == "__main__":
    ingest_data()