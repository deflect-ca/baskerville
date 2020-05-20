from baskerville.features.updateable_features import UpdaterRatio
from pyspark.sql import functions as F

from baskerville.features.feature_request_total import FeatureRequestTotal
from baskerville.features.feature_unique_ua_total import \
    FeatureUniqueUaTotal
from baskerville.features.helpers import update_ratio


class FeatureUniqueUaToRequestRatio(UpdaterRatio):
    """
    For each IP compute the ratio of unique user agents used to total requests.
    """
    DEFAULT_VALUE = 1.
    COLUMNS = ['client_ua', '@timestamp']
    DEPENDENCIES = [FeatureRequestTotal, FeatureUniqueUaTotal]

    def __init__(self):
        super(FeatureUniqueUaToRequestRatio, self).__init__()

        self.group_by_aggs = {
            'distinct_ua': F.countDistinct(F.col('client_ua')).cast('float'),
            'num_requests': F.count(F.col('@timestamp')).cast('float'),
        }

    def compute(self, df):
        from pyspark.sql import functions as F

        df = df.withColumn(
            self.feature_name,
            (F.col('distinct_ua').cast('float') /
             F.col('num_requests').cast('float')).cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_ratio(
                    past.get(FeatureUniqueUaTotal.feature_name_from_class()),
                    past.get(FeatureRequestTotal.feature_name_from_class()),
                    current[FeatureUniqueUaTotal.feature_name_from_class()],
                    current[FeatureRequestTotal.feature_name_from_class()]
                )

    def update(self, df, feat_column='features', old_feat_column='old_features'):
        return super().update(
            df,
            FeatureUniqueUaTotal.feature_name_from_class(),
            FeatureRequestTotal.feature_name_from_class(),
        )
