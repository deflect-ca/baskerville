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
    T.StructField("uuid_request_set", T.StringType(), False),
    T.StructField("features", T.StringType(), False)
])

prediction_schema = T.StructType([
    T.StructField("id_client", T.StringType(), False),
    T.StructField("uuid_request_set", T.StringType(), False),
    T.StructField("prediction", T.FloatType(), False),
    T.StructField("score", T.FloatType(), False),
    T.StructField("classifier_score", T.FloatType(), False)
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


def get_message_schema(all_features: dict) -> T.StructType:
    schema = T.StructType([
        T.StructField("id_client", T.StringType(), True),
        T.StructField("uuid_request_set", T.StringType(), False)
    ])
    schema.add(T.StructField("features", get_features_schema(all_features)))
    return schema


def get_features_schema(all_features: dict) -> T.StructType:
    features = T.StructType()
    for feature in all_features.keys():
        features.add(T.StructField(
            name=feature,
            dataType=T.StringType(),
            nullable=True))
    return features


def get_data_schema() -> T.StructType:
    """
    Return the kafka data schema
    """
    return T.StructType(
        [T.StructField('key', T.StringType()),
         T.StructField('message', T.StringType())]
    )


def get_feedback_context_schema() -> T.StructType:
    return T.StructType(
        [T.StructField('id', T.IntegerType()),
         T.StructField('uuid_organization', T.StringType()),
         T.StructField('reason', T.StringType()),
         T.StructField('reason_descr', T.StringType()),
         T.StructField('start', T.StringType()),
         T.StructField('stop', T.StringType()),
         T.StructField('ip_count', T.IntegerType()),
         T.StructField('notes', T.StringType()),
         T.StructField('progress_report', T.StringType()),
         T.StructField('pending', T.BooleanType()),
         ]
    )


def get_submitted_feedback_schema() -> T.ArrayType:
    return T.ArrayType(
            T.StructType([
                T.StructField('id', T.IntegerType()),
                T.StructField('id_context', T.IntegerType()),
                T.StructField('uuid_organization', T.StringType()),
                T.StructField('uuid_request_set', T.StringType()),
                T.StructField('prediction', T.IntegerType()),
                T.StructField('score', T.FloatType()),
                T.StructField('attack_prediction', T.FloatType()),
                T.StructField('low_rate', T.BooleanType()),
                feature_vectors_schema,
                T.StructField('feedback', T.StringType()),
                T.StructField('start', T.StringType()),
                T.StructField('submitted_at', T.StringType()),
                T.StructField('created_at', T.StringType()),
                T.StructField('updated_at', T.StringType())
            ])
        )


NAME_TO_SCHEMA = {
    'FeedbackSchema': {
        'feedback_context': get_feedback_context_schema(),
        'feedback': get_submitted_feedback_schema(),
    }
}
