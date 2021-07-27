# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import pickle
from collections import namedtuple
# import numpy as np


def update_mean(v_old, v_cur, n_old, n_cur):
    return (n_old*v_old + n_cur*v_cur) / float(n_old + n_cur)


def update_variance(v_old, v_cur, n_old, n_cur, m_old, m_cur):
    m_new = update_mean(v_old, v_cur, n_old, n_cur)
    v_new = ((n_old - 1)*v_old**2 +
             (n_cur - 1)*v_cur**2 +
             n_old*(m_old - m_new)**2 +
             n_cur*(m_cur - m_new)**2) / float(n_old + n_cur - 1)
    return v_new


def update_total(v_old, v_cur):
    return v_old + v_cur


def update_rate(total_old, total_cur, minutes_total_cur):
    if minutes_total_cur > 0:
        return (total_old + total_cur) / float(minutes_total_cur)
    # total_cur will hold the default value if minutes_total_cur is 0.
    return total_cur


# def update_maximum(v_old, v_cur):
#     return np.maximum(v_old, v_cur)
#
#
# def update_minimum(v_old, v_cur):
#     return np.minimum(v_old, v_cur)


def update_ratio(numerator_old, denom_old, numerator_cur, denom_cur):
    if denom_old + denom_cur > 0.:
        return (numerator_old + numerator_cur) / float(denom_old + denom_cur)
    else:
        return (numerator_old + numerator_cur) / 0.01


def update_replace(v_cur):
    return v_cur


def update_features(
        active_features, features_cur, features_past, subset_count,
        start, last_request
):
    """
    Given the current and past feature dicts, and a list of the active
    features, return a dictionary with the feature values updated properly

    :param list[T] active_features:
    :param dict[str, float] features_cur:
    :param dict[str, float] features_past:
    :param int subset_count:
    :param timestamp start:
    :param timestamp last_request:
    :return:
    :rtype: dict[str, float]
    """
    updated_features = {}

    # if serialized
    if not isinstance(active_features, list):
        active_features = pickle.loads(active_features)

    for f in active_features:
        if subset_count > 0:
            updated_features[f.feature_name] = f.update_row(
                features_cur,
                features_past,
                subset_count=subset_count,
                start=start,
                last_request=last_request
            )
        else:
            updated_features[f.feature_name] = features_cur[f.feature_name]

    return updated_features


def extract_features_in_order(feature_dict, model_features):
    """
    Returns the model features in the order the model requires them.
    """
    return [feature_dict[feature] for feature in model_features]


def calculate_dt(
        df,
        start_col='start',
        end_col='last_request',
        format="YYYY-MM-DD %H:%M:%S"
):
    from pyspark.sql import functions as F
    if start_col not in df.columns or end_col not in df.columns:
        raise ValueError('Cannot calculate dt, missing start or end columns ')
    return df.withColumn(
        'dt',
        (F.unix_timestamp(F.col(end_col).cast('timestamp'), format=format) -
         F.unix_timestamp(F.col(start_col), format=format)
         ).cast('float') / 60.
    )


def feature_is_updateable(feature_instance):
    # todo: there has to be a better way...
    return hasattr(feature_instance, 'updated_feature_col_name')


# WST
StrippedFeature = namedtuple('StrippedFeature', ['feature_name', 'update_row'])
