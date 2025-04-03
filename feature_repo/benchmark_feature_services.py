from feast import FeatureService

from benchmark_feature_views import benchmark_features_stream, benchmark_test_fv_stream, benchmark_stats_fv

feature_service = FeatureService(
    name="benchmark_feature_service",
    features=[
        benchmark_stats_fv
    ]
)