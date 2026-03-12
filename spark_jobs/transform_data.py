import os
import time
import logging
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper

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
DATA_LAKE = os.environ.get("DATA_LAKE_PATH", "/opt/airflow/data_lake")
RAW_PARQUET = os.path.join(DATA_LAKE, "raw/transaction_data.parquet")
PROCESSED_PARQUET = os.path.join(DATA_LAKE, "processed/finance_processed.parquet")

# ------------------------------
# Transform function
# ------------------------------
def transform():
    logger.info("Starting transformation...")

    spark = SparkSession.builder \
        .appName("FinanceTransform") \
        .getOrCreate()

    # ------------------------------
    # Read raw parquet
    # ------------------------------
    if not os.path.exists(RAW_PARQUET):
        logger.error(f"Raw parquet file not found: {RAW_PARQUET}")
        spark.stop()
        return

    logger.info(f"Reading raw parquet from {RAW_PARQUET}...")
    df_raw = spark.read.parquet(RAW_PARQUET)
    raw_count = df_raw.count()
    logger.info(f"Total raw records: {raw_count}")

    if raw_count == 0:
        logger.warning("Raw dataset is empty!")
        spark.stop()
        return

    # ------------------------------
    # Basic cleaning
    # ------------------------------
    logger.info("Cleaning and standardizing data...")
    df_clean = (
        df_raw.select(
            col("transaction_id").cast("long"),
            trim(col("customer_name")).alias("customer_name"),
            trim(col("customer_email")).alias("customer_email"),
            trim(col("merchant_name")).alias("merchant_name"),
            col("transaction_amount").cast("double"),
            col("transaction_date").cast("date")
        )
        .withColumn("customer_name", upper(col("customer_name")))
        .withColumn("merchant_name", upper(col("merchant_name")))
    )

    # ------------------------------
    # Deduplicate against processed parquet
    # ------------------------------
    df_new = df_clean
    df_existing = None

    if os.path.exists(PROCESSED_PARQUET):
        logger.info("Existing processed parquet found. Deduplicating new records...")
        df_existing = spark.read.parquet(PROCESSED_PARQUET)
        existing_count = df_existing.count()
        logger.info(f"Existing processed count: {existing_count}")

        if existing_count > 0:
            df_new = df_clean.join(
                df_existing.select("transaction_id"),
                on="transaction_id",
                how="left_anti"  # Keep only new records
            )
            logger.info(f"New unique records to append: {df_new.count()}")
        else:
            logger.warning("Existing parquet is empty. Skipping deduplication.")

    # ------------------------------
    # Merge old + new safely
    # ------------------------------
    if df_existing is not None and df_new.count() > 0:
        df_final = df_existing.unionByName(df_new)
    elif df_existing is not None:
        logger.info("No new records. Keeping existing processed parquet.")
        df_final = df_existing
    else:
        df_final = df_new

    # ------------------------------
    # Save safely using temp folder
    # ------------------------------
    temp_folder = PROCESSED_PARQUET + "_tmp_" + str(int(time.time()))
    os.makedirs(os.path.dirname(temp_folder), exist_ok=True)
    logger.info(f"Writing processed data to temp folder {temp_folder}...")

    df_final.write.mode("overwrite").parquet(temp_folder)

    if os.path.exists(PROCESSED_PARQUET):
        logger.info(f"Deleting old processed parquet at {PROCESSED_PARQUET}...")
        shutil.rmtree(PROCESSED_PARQUET)

    os.rename(temp_folder, PROCESSED_PARQUET)
    logger.info(f"Processed data saved to {PROCESSED_PARQUET}")

    # ------------------------------
    # Final validation
    # ------------------------------
    df_check = spark.read.parquet(PROCESSED_PARQUET)
    final_count = df_check.count()
    logger.info(f"Final processed records count: {final_count}")

    spark.stop()
    logger.info("Transformation completed successfully.")

# ------------------------------
# Entry point
# ------------------------------
if __name__ == "__main__":
    transform()