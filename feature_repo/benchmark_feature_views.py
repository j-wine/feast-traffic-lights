from datetime import timedelta
from datetime import timedelta

from feast import Field
from feast.stream_feature_view import stream_feature_view
from feast.types import Int64
from pyspark.sql import DataFrame

from data_sources import benchmark_stream_source
from entities import benchmark_entity

schema = [
    Field(name="feature_0", dtype=Int64),
    Field(name="feature_1", dtype=Int64),
    Field(name="feature_2", dtype=Int64),
    Field(name="feature_3", dtype=Int64),
    Field(name="feature_4", dtype=Int64),
    Field(name="feature_5", dtype=Int64),
    Field(name="feature_6", dtype=Int64),
    Field(name="feature_7", dtype=Int64),
    Field(name="feature_8", dtype=Int64),
    Field(name="feature_9", dtype=Int64)
]

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
    from pyspark.sql.functions import col, sum, reduce

    df = df.withColumn("sum", col("feature_0") + col("feature_9"))
    df = df.select("entity", "event_timestamp", "sum")
    return df
