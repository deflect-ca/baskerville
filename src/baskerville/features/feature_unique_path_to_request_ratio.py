from baskerville.features.updateable_features import UpdaterRatio
from pyspark.sql import functions as F

from baskerville.features.feature_request_total import FeatureRequestTotal
from baskerville.features.feature_unique_path_total import \
    FeatureUniquePathTotal
from baskerville.features.helpers import update_ratio


class FeatureUniquePathToRequestRatio(UpdaterRatio):
    """
    For each IP compute the ratio of unique paths to total requests.
    """
    DEFAULT_VALUE = 1.
    COLUMNS = ['client_url', '@timestamp']
    DEPENDENCIES = [FeatureRequestTotal, FeatureUniquePathTotal]

    def __init__(self):
        super(FeatureUniquePathToRequestRatio, self).__init__()

        self.group_by_aggs = {
            'unique_urls': F.countDistinct(F.col('client_url')),
            'num_requests': F.count(F.col('@timestamp')).cast('float'),
        }

    def compute(self, df):
        from pyspark.sql import functions as F

        df = df.withColumn(
            self.feature_name,
            (F.col('unique_urls').cast('float') /
             F.col('num_requests').cast('float')).cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_ratio(
            past.get(FeatureUniquePathTotal.feature_name_from_class()),
            past.get(FeatureRequestTotal.feature_name_from_class()),
            current[FeatureUniquePathTotal.feature_name_from_class()],
            current[FeatureRequestTotal.feature_name_from_class()]
        )

    def update(self, df, feat_column='features', old_feat_column='old_features'):
        return super().update(
            df,
            FeatureUniquePathTotal.feature_name_from_class(),
            FeatureRequestTotal.feature_name_from_class(),
        )
