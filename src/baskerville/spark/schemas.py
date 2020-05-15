from pyspark.sql import types as T


cross_reference_schema = T.StructType([
    T.StructField("label", T.IntegerType(), False),
    T.StructField("id_attribute", T.IntegerType(), False)
])

prediction_schema = T.StructType([
    T.StructField("prediction", T.FloatType(), False),
    T.StructField("score", T.FloatType(), True),
    T.StructField("threshold", T.FloatType(), True)
])


def get_models_schema():
    return T.StructType([
                T.StructField("features", T.ArrayType(T.StringType()), True),
                T.StructField("algorithm", T.StringType(), True),
                T.StructField("scaler_type", T.StringType(), True),
                T.StructField("parameters", T.StringType(), True),
                T.StructField("recall", T.DoubleType(), True),
                T.StructField("precision", T.DoubleType(), True),
                T.StructField("f1_score", T.DoubleType(), True),
                T.StructField("classifier", T.BinaryType(), True),
                T.StructField("scaler", T.BinaryType(), True),
                T.StructField("n_training", T.IntegerType(), True),
                T.StructField("n_testing", T.IntegerType(), True),
                T.StructField("threshold", T.DoubleType(), True)
            ])


def get_cache_schema():
    return T.StructType([
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
