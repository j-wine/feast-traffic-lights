from datetime import timedelta
from feast import Field, FeatureService, FeatureView, ValueType, Feature
from feast.stream_feature_view import stream_feature_view
from pyspark.sql import DataFrame
from feast.types import Int64
from data_sources import benchmark_stream_source
from entities import benchmark_entity

benchmark_stats_fv = FeatureView(
    name="benchmark_stats_fv",
    entities=[benchmark_entity],
    ttl=timedelta(days=14),
    schema=[
        Field(name="feature_0", dtype=Int64),
        Field(name="feature_1", dtype=Int64),
    ],
    source=benchmark_stream_source,
    online=True

)

@stream_feature_view(
    entities=[benchmark_entity],
    ttl=timedelta(days=140),
    mode="spark",  # apparently spark is currently the only support "mode"
    schema=[
        Field(name="sum", dtype=Int64),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=benchmark_stream_source,
)
def benchmark_features_stream(df: DataFrame):
    from pyspark.sql.functions import col
    print("df in benchmark_features_stream:", df)
    df = df.withColumn("sum", col("feature_0") + col("feature_9"))
    return df

@stream_feature_view(
    entities=[benchmark_entity],
    ttl=timedelta(days=140),
    mode="spark",  # apparently spark is currently the only support "mode"
    schema=[
        Field(name="sum2", dtype=Int64),
    ],
    timestamp_field="event_timestamp",
    online=True,
    source=benchmark_stream_source,
)
def benchmark_test_fv_stream(df: DataFrame):
    from pyspark.sql.functions import col

    df = df.withColumn("sum", col("feature_1") + col("feature_2"))
    df = df.select("entity", "event_timestamp", "sum2")
    return df
