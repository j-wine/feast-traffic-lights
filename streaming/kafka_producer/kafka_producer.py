import json
import time
import threading
import pandas as pd
from kafka import KafkaProducer
from feast import FeatureStore

KAFKA_TOPIC = "benchmark_signals"

# -------- Configuration --------
KAFKA_BROKERS = ["broker-1:9092", "broker-2:9093"]
PARQUET_FILE = "/app/offline_data/generated_data.parquet"
ENTITY_NAME = "benchmark_entity"
FEATURE_VIEW = "benchmark_features_stream"
RESULT_CSV = "/app/latency_results.csv"

store = FeatureStore()

def poll_until_available(entity_id: int, start_time: float):
    """Poll Feast every second for feature availability and log latency."""
    for _ in range(60):
        result = store.get_online_features(
            features=[f"{FEATURE_VIEW}:sum"],
            entity_rows=[{ENTITY_NAME: entity_id}]
        ).to_dict()


        val = result[f"{FEATURE_VIEW}:sum"][0]
        if val is not None:
            latency = time.time() - start_time
            print(f"✅ {entity_id} ready in {latency:.3f}s: {val}")
            with open(RESULT_CSV, "a") as f:
                f.write(f"{entity_id},{start_time:.6f},{latency:.3f},{val}\n")
            return
        time.sleep(0.1)

    print(f"❌ Timeout for {entity_id}")


def main():
    df = pd.read_parquet(PARQUET_FILE)
    print(f"📦 Loaded {len(df)} rows from {PARQUET_FILE}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    for idx, row in df.iterrows():
        entity_id = idx  # Must be int, per schema_json

        payload = {
            "benchmark_entity": entity_id,
            "event_timestamp": row["event_timestamp"].isoformat(),
            **{f"feature_{i}": int(row[f"feature_{i}"]) for i in range(10)}
        }

        send_time = time.time()
        producer.send(KAFKA_TOPIC, value=payload)
        print(f"📤 Sent {entity_id} at {send_time:.6f}")

        threading.Thread(
            target=poll_until_available,
            args=(entity_id, send_time),
            daemon=True
        ).start()

        time.sleep(0.5)  # Control rate if needed

    producer.flush()
    print("🛑 All rows sent. Waiting for polling threads...")
    time.sleep(65)


if __name__ == "__main__":
    main()