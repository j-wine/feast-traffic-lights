import pandas as pd
import time
from feast import FeatureStore
from datetime import datetime

# -------- Configuration --------
ENTITY_NAME = "benchmark_entity"
FEATURE_VIEW = "benchmark_features_stream"
FEATURE_NAME = "feature_0"
PARQUET_FILE = "offline_data/generated_data.parquet"
RESULT_CSV = "/app/historical_results.csv"

# Initialize Feast
store = FeatureStore()

# Read offline data for the entity rows
df = pd.read_parquet(PARQUET_FILE)

# Choose N rows for testing (you can increase this as needed)
df = df.head(10)

# Build entity DataFrame for historical retrieval
entity_df = pd.DataFrame({
    ENTITY_NAME: df[ENTITY_NAME],
    "event_timestamp": df["event_timestamp"],
})

# Time the historical retrieval
start_time = time.time()

retrieval_job = store.get_historical_features(
    entity_df=entity_df,
    features=[f"{FEATURE_VIEW}:{FEATURE_NAME}"],
)

result_df = retrieval_job.to_df()
end_time = time.time()

# Save results
result_df.to_csv(RESULT_CSV, index=False)
print(f"✅ Retrieved {len(result_df)} rows in {end_time - start_time:.3f}s → saved to {RESULT_CSV}")
