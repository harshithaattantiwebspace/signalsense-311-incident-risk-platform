import json
import math
from typing import List

from kafka import KafkaConsumer
from google.cloud import bigquery

# -----------------------------
# Config
# -----------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "issues_raw"
GROUP_ID = "issues_consumer_group"

# Update these to match your GCP setup
BQ_PROJECT = "signalsense-project"
BQ_DATASET = "signalsense"
BQ_TABLE = "issues_enriched_stream"

# How many messages to buffer before inserting into BigQuery
BATCH_SIZE = 100


def clean_record(record: dict) -> dict:
    """
    Replace NaN / infinities with None so the JSON we send to BigQuery is valid.
    """
    clean = {}
    for k, v in record.items():
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            clean[k] = None
        else:
            clean[k] = v
    return clean


def insert_into_bigquery(rows: List[dict]):
    """
    Insert a batch of JSON rows into BigQuery.
    """
    if not rows:
        return

    client = bigquery.Client(project=BQ_PROJECT)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    print(f"Attempting to insert {len(rows)} rows into {table_id}...")

    errors = client.insert_rows_json(table_id, rows)
    if errors:
        print("❌ BigQuery insert errors:")
        for e in errors:
            print(e)
    else:
        print(f"✅ Inserted {len(rows)} rows into {table_id}")


def main():
    print("Starting Kafka consumer...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        enable_auto_commit=True,
    )

    buffer: List[dict] = []

    try:
        for message in consumer:
            record = message.value

            # Debug: show a few messages to prove we're consuming
            if len(buffer) < 3:
                print("Got message from Kafka, raw issue_id:", record.get("issue_id"))

            # Ensure issue_id is a string (safer for BigQuery STRING type)
            if "issue_id" in record:
                record["issue_id"] = str(record["issue_id"])

            # Clean NaN / inf -> None
            record = clean_record(record)

            buffer.append(record)

            if len(buffer) >= BATCH_SIZE:
                insert_into_bigquery(buffer)
                buffer = []

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        # Flush any remaining rows
        if buffer:
            insert_into_bigquery(buffer)


if __name__ == "__main__":
    main()
