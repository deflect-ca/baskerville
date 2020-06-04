# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.features.updateable_features import UpdaterMean
from pyspark.sql import functions as F

from baskerville.features.feature_request_total import FeatureRequestTotal
from baskerville.features.helpers import update_mean


class FeaturePayloadSizeLogAverage(UpdaterMean):
    """
    For each IP compute the average of the log of the payload size in bytes.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['reply_length_bytes']
    DEPENDENCIES = [FeatureRequestTotal]

    def __init__(self):
        super(FeaturePayloadSizeLogAverage, self).__init__()

        self.group_by_aggs = {
            'reply_length_log': F.avg(
                F.log(F.col('reply_length_bytes') + 1.)
            ).cast('float')
        }

    def compute(self, df):
        from pyspark.sql import functions as F

        df = df.withColumn(
            self.feature_name,
            F.col('reply_length_log').cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_mean(
            past.get(cls.feature_name_from_class()),
            current[cls.feature_name_from_class()],
            past.get(FeatureRequestTotal.feature_name_from_class()),
            current[FeatureRequestTotal.feature_name_from_class()]
        )

    def update(self, df, feat_column='features', old_feat_column='old_features'):
        return super().update(
            df,
            self.feature_name,
            FeatureRequestTotal.feature_name_from_class()
        )
