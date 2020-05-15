from baskerville.features.updateable_features import UpdaterRate
from pyspark.sql import functions as F

from baskerville.features.base_feature import TimeBasedFeature
from baskerville.features.feature_minutes_total import FeatureMinutesTotal
from baskerville.features.feature_unique_path_total import \
    FeatureUniquePathTotal
from baskerville.features.helpers import update_rate


class FeatureUniquePathRate(TimeBasedFeature, UpdaterRate):
    """
    For each IP compute the number of unique paths per minute.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['client_url', '@timestamp']
    DEPENDENCIES = [FeatureMinutesTotal, FeatureUniquePathTotal]

    def __init__(self):
        super(FeatureUniquePathRate, self).__init__()

        self.group_by_aggs.update({
            'unique_urls':  F.countDistinct(F.col('client_url')),
        })

    def compute(self, df):

        df = df.withColumn(
            self.feature_name,
            F.when(F.col('dt') != 0.,
                   (F.col('unique_urls').cast('float') /
                    F.col('dt').cast('float')).cast('float')
                   ).otherwise(F.lit(self.feature_default).cast('float'))
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_rate(
                    past.get(FeatureUniquePathTotal.feature_name_from_class()),
                    current[FeatureUniquePathTotal.feature_name_from_class()],
                    current[FeatureMinutesTotal.feature_name_from_class()]
                )

    def update(self, df):
        return super().update(
            df,
            FeatureMinutesTotal.feature_name_from_class(),
            numerator=FeatureUniquePathTotal.feature_name_from_class()
        )
