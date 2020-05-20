from baskerville.features.updateable_features import UpdaterMean
from pyspark.sql import functions as F

from baskerville.features.feature_request_total import FeatureRequestTotal
from baskerville.features.helpers import update_mean


class FeaturePathDepthAverage(UpdaterMean):
    """
    For each IP compute the average depth of all requested paths.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['client_url']
    DEPENDENCIES = [FeatureRequestTotal]

    def __init__(self):
        super(FeaturePathDepthAverage, self).__init__()

        self.group_by_aggs = {
            'client_url_slash_count_avg': F.avg(
                F.col('client_url_slash_count')
            ),
        }
        self.pre_group_by_calcs = {
            'client_url_slash_count': (
                    F.size(F.split(F.col('client_url'), '/')) - 1
            )
        }

    def compute(self, df):

        df = df.withColumn(
            self.feature_name,
            F.col('client_url_slash_count_avg').cast('float')
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
