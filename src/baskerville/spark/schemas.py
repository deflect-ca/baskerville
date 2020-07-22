# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from pyspark.sql import types as T


cross_reference_schema = T.StructType([
    T.StructField("label", T.IntegerType(), False),
    T.StructField("id_attribute", T.IntegerType(), False)
])

features_schema = T.StructType([
    T.StructField("id_client", T.StringType(), True),
    T.StructField("id_request_sets", T.StringType(), False),
    T.StructField("features", T.StringType(), False)
])

prediction_schema = T.StructType([
    T.StructField("id_client", T.StringType(), False),
    T.StructField("id_request_sets", T.StringType(), False),
    T.StructField("prediction", T.FloatType(), False),
    T.StructField("score", T.FloatType(), False)
])

feature_vectors_schema = T.StructField(
    'features', T.MapType(T.StringType(), T.FloatType()), False
)

rs_cache_schema = T.StructType([
    T.StructField("id", T.IntegerType(), False),
    T.StructField("target", T.StringType(), False),
    T.StructField("ip", T.StringType(), False),
    T.StructField("first_ever_request", T.TimestampType(), True),
    T.StructField("old_subset_count", T.IntegerType(), True),
    T.StructField("old_features",
                  T.MapType(T.StringType(), T.DoubleType()), True),
    T.StructField("old_num_requests", T.IntegerType(), True),
    T.StructField("updated_at", T.TimestampType(), True)
])
