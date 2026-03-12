import csv
import random
from faker import Faker
from datetime import datetime
from itertools import islice

fake = Faker()
OUTPUT_FILE = "/home/waleed/my-etl-pipeline/finance-data-pipeline/data_lake/raw/transaction_data.csv"
NUM_RECORDS = 1_000_000
BATCH_SIZE = 50_000  # write in batches to speed up

def generate_batch(batch_num, batch_size):
    batch = []
    start_index = batch_num * batch_size
    for i in range(start_index, start_index + batch_size):
        batch.append([
            i,
            fake.name(),
            fake.email(),
            fake.company(),
            round(random.uniform(5, 5000), 2),
            fake.date_between(start_date="-2y", end_date="today")
        ])
    return batch

def generate_data():
    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        # Write header
        writer.writerow([
            "transaction_id",
            "customer_name",
            "customer_email",
            "merchant_name",
            "transaction_amount",
            "transaction_date"
        ])

        num_batches = NUM_RECORDS // BATCH_SIZE
        for batch_num in range(num_batches):
            batch = generate_batch(batch_num, BATCH_SIZE)
            writer.writerows(batch)
            print(f"{(batch_num+1)*BATCH_SIZE} records generated")

        # write remaining records if any
        remaining = NUM_RECORDS % BATCH_SIZE
        if remaining:
            batch = generate_batch(num_batches, remaining)
            writer.writerows(batch)
            print(f"{NUM_RECORDS} records generated")

    print("1M finance records generated successfully!")

if __name__ == "__main__":
    generate_data()