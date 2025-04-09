import json
import random
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

TRAFFIC_LIGHT_TOPIC = "traffic_light_signals"
BENCHMARK_TOPIC = "benchmark_entity_topic"
KAFKA_BROKERS = ["broker-1:9092"]

def read_benchmark_data():
    parquet_file = Path(__file__).parent / "offline_data/generated_data.parquet"
    df = pd.read_parquet(parquet_file)
    df = df.sort_values("benchmark_entity")  # Sorting the DataFrame by 'benchmark_entity'
    return df

def generate_traffic_light_data():
    traffic_light_id = random.randint(1, 5)

    primary_signal = random.randint(1, 3)
    secondary_signal = random.randint(1, 3)

    timestamp = (datetime.now(timezone.utc) + timedelta(seconds=random.randint(0, 30))).isoformat()

    return {
        "traffic_light_id": str(traffic_light_id),
        "primary_signal": primary_signal,
        "secondary_signal": secondary_signal,
        "location": "Dammtor/Theodor-Heuss-Platz",
        "event_timestamp": timestamp
    }

def produce_kafka_messages():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    benchmark_df = read_benchmark_data()
    benchmark_iter = iter(benchmark_df.itertuples(index=False, name=None))  # Use itertuples for better performance
    print("Producing traffic light signals to Kafka...")
    while True:
        # traffic_light_data  = generate_traffic_light_data()
        # producer.send(TRAFFIC_LIGHT_TOPIC, traffic_light_data)

        # Send one benchmark entity if available
        try:
            benchmark_data = next(benchmark_iter)
            benchmark_entity = {
                "benchmark_entity": benchmark_data[0],
                "event_timestamp": benchmark_data[1].isoformat(),
                "feature_0": benchmark_data[2],
                "feature_1": benchmark_data[3],
                "feature_2": benchmark_data[4],
                "feature_3": benchmark_data[5],
                "feature_4": benchmark_data[6],
                "feature_5": benchmark_data[7],
                "feature_6": benchmark_data[8],
                "feature_7": benchmark_data[9],
                "feature_8": benchmark_data[10],
                "feature_9": benchmark_data[11]
            }
            producer.send(BENCHMARK_TOPIC, benchmark_entity)
            print(f"Sent Benchmark Data: {benchmark_entity}")
        except StopIteration:
            print("All benchmark entities have been sent.")
            break

        time.sleep(1)

if __name__ == "__main__":
    produce_kafka_messages()
