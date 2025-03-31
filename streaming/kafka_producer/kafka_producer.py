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
FEATURE_NAME = "benchmark_features_stream:sum"
ENTITY_NAME = "benchmark_entity"
# -------- Kafka Setup --------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

store = FeatureStore()


def poll_feast_availability(entity_id: str, start_time: float):
    """Poll Feast every second for feature availability."""
    for _ in range(60):
        result = store.get_online_features(
            features=[FEATURE_NAME],
            entity_rows=[{ENTITY_NAME: entity_id}]
        ).to_dict()

        if result[FEATURE_NAME][0] is not None:
            latency = time.time() - start_time
            print(f"✅ {entity_id} available after {latency:.3f}s")
            with open("latency_results.csv", "a") as f:
                f.write(f"{entity_id},{start_time:.6f},{latency:.3f},{result[FEATURE_NAME][0]:.3f}\n")
            return
        time.sleep(0.1)

    print(f"❌ Timeout for {entity_id}")


def main():
    df = pd.read_parquet(PARQUET_FILE)
    print(f"📦 Loaded {len(df)} rows from {PARQUET_FILE}")

    for idx, row in df.iterrows():
        entity_id = f"{ENTITY_NAME}_{idx:03d}"
        payload = {
            "benchmark_entity": entity_id,
            "feature_x": row["feature_x"],
            "event_timestamp": row["event_timestamp"].isoformat()
        }

        send_time = time.time()
        producer.send(KAFKA_TOPIC, value=payload)
        print(f"📤 Sent {entity_id} at {send_time:.6f}")

        threading.Thread(
            target=poll_feast_availability,
            args=(entity_id, send_time),
            daemon=True
        ).start()

        time.sleep(0.5)  # Small gap between sends to avoid flooding

    producer.flush()
    print("🛑 Done sending. Waiting for all polls to finish...")
    time.sleep(65)  # Wait for all polls to complete


if __name__ == "__main__":
    main()
