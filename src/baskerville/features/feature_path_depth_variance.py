from baskerville.features.updateable_features import UpdaterVariance
from pyspark.sql import functions as F

from baskerville.features.feature_request_total import FeatureRequestTotal
from baskerville.features.feature_path_depth_average import \
    FeaturePathDepthAverage
from baskerville.features.helpers import update_variance


class FeaturePathDepthVariance(UpdaterVariance):
    DEFAULT_VALUE = 0.
    COLUMNS = ['client_url']
    DEPENDENCIES = [FeatureRequestTotal, FeaturePathDepthAverage]

    def __init__(self):
        super(FeaturePathDepthVariance, self).__init__()
        self.group_by_aggs = {
            'client_url_slash_count_variance':  F.variance(
                F.col('client_url_slash_count')
            )
        }
        self.pre_group_by_calcs = {
            'client_url_slash_count': (
                    F.size(F.split(F.col('client_url'), '/')) - 1
            )
        }

    def compute(self, df):
        from pyspark.sql import functions as F
        df = df.withColumn(
            self.feature_name,
            F.when(
                F.isnull(F.col('client_url_slash_count_variance')) |
                F.isnan(F.col('client_url_slash_count_variance')),
                F.lit(self.feature_default).cast('float')
            ).otherwise(
                F.col('client_url_slash_count_variance').cast('float')
            )
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_variance(
            past.get(cls.feature_name_from_class()),
            current[cls.feature_name_from_class()],
            past.get(FeatureRequestTotal.feature_name_from_class()),
            current[FeatureRequestTotal.feature_name_from_class()],
            past.get(FeaturePathDepthAverage.feature_name_from_class()),
            current[FeaturePathDepthAverage.feature_name_from_class()]
        )

    def update(self, df, feat_column='features', old_feat_column='old_features'):
        return super().update(
            df,
            self.feature_name,
            FeatureRequestTotal.feature_name_from_class(),
            FeaturePathDepthAverage.feature_name_from_class()
        )

