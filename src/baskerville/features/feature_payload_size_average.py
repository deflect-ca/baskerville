from baskerville.features.updateable_features import UpdaterMean
from pyspark.sql import functions as F

from baskerville.util.enums import FeatureComputeType
from baskerville.features.helpers import update_mean
from baskerville.features.feature_request_total import FeatureRequestTotal


class FeaturePayloadSizeAverage(UpdaterMean):
    """
    For each IP compute the average payload size in kilobytes.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['reply_length_bytes']
    DEPENDENCIES = [FeatureRequestTotal]

    def __init__(self):
        super(FeaturePayloadSizeAverage, self).__init__()

        self.group_by_aggs = {
            'reply_length_bytes_avg': F.avg(
                0.001 * F.col('reply_length_bytes')
            ).cast('float')
        }

    def compute(self, df):
        from pyspark.sql import functions as F

        df = df.withColumn(
            self.feature_name,
            F.col('reply_length_bytes_avg').cast('float')
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
