import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

CSV_PATH = Path("../data/issues_enriched_local.csv")  # adjust if needed
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "issues_raw"

def main():
    print("Loading data from:", CSV_PATH)
    df = pd.read_csv(CSV_PATH)

    print(f"Total rows to send: {len(df)}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
    )

    for idx, row in df.iterrows():
        # Convert each row to a plain dict (JSON-friendly)
        record = row.to_dict()
        key = record.get("issue_id", idx)

        producer.send(
            TOPIC_NAME,
            key=key,
            value=record
        )

        # Flush occasionally
        if idx % 1000 == 0:
            producer.flush()
            print(f"Sent {idx} messages...")

        # Optional: slow down to simulate real-time
        # time.sleep(0.01)

    producer.flush()
    print("Finished sending all messages.")

if __name__ == "__main__":
    main()
