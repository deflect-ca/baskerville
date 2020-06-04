# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.features.updateable_features import UpdaterRatio
from pyspark.sql import functions as F

from baskerville.features.feature_request_total import FeatureRequestTotal
from baskerville.features.feature_response4xx_total import \
    FeatureResponse4xxTotal
from baskerville.features.helpers import update_ratio


class FeatureResponse4xxToRequestRatio(UpdaterRatio):
    """
    For each IP compute the ratio of 4xx response codes to total requests.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['http_response_code', '@timestamp']
    DEPENDENCIES = [FeatureRequestTotal, FeatureResponse4xxTotal]

    def __init__(self):
        super(FeatureResponse4xxToRequestRatio, self).__init__()

        self.group_by_aggs = {
            '4xx': F.count(F.when(F.col('4xx') == True, F.col('4xx'))),  # noqa
            'num_requests': F.count(F.col('@timestamp')).cast('float'),
        }
        self.pre_group_by_calcs = {
            'response_code_category': F.floor(
                F.col('http_response_code') / 100.),
            '4xx': F.col('response_code_category') == 4,
        }

    def compute(self, df):
        from pyspark.sql import functions as F

        df = df.withColumn(
            self.feature_name,
            (F.col('4xx').cast('float') / F.col('num_requests').cast('float')
             ).cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_ratio(
            past.get(FeatureResponse4xxTotal.feature_name_from_class()),
            past.get(FeatureRequestTotal.feature_name_from_class()),
            current[FeatureResponse4xxTotal.feature_name_from_class()],
            current[FeatureRequestTotal.feature_name_from_class()]
        )

    def update(self, df, feat_column='features', old_feat_column='old_features'):
        return super().update(
            df,
            FeatureResponse4xxTotal.feature_name_from_class(),
            FeatureRequestTotal.feature_name_from_class(),
        )
