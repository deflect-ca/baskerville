# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.features.updateable_features import UpdaterTotal
from pyspark.sql import functions as F
from baskerville.features.helpers import update_total


class FeatureResponse4xxTotal(UpdaterTotal):
    """
    For each IP compute the total number of 4xx response codes.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['http_response_code']
    DEPENDENCIES = []

    def __init__(self):
        super(FeatureResponse4xxTotal, self).__init__()

        self.group_by_aggs = {
            '4xx': F.count(F.when(F.col('4xx') == True, F.col('4xx')))  # noqa
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
            (F.col('4xx')).cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_total(
            current[cls.feature_name_from_class()],
            past.get(cls.feature_name_from_class())
        )
