from feast import Entity, ValueType
from feast.types import Int64

benchmark_entity = Entity(
    name="benchmark_entity",
    value_type=ValueType.STRING,
)

