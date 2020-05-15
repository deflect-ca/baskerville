from baskerville.features.updateable_features import UpdaterTotal
from pyspark.sql import functions as F

from baskerville.features.base_feature import BaseFeature
from baskerville.features.helpers import update_total


class FeatureUniqueQueryTotal(UpdaterTotal):
    """
    For each IP compute the total number of unique queries.
    """
    DEFAULT_VALUE = 1.
    COLUMNS = ['querystring']
    DEPENDENCIES = []

    def __init__(self):
        super(FeatureUniqueQueryTotal, self).__init__()

        self.group_by_aggs = {
            'num_unique_queries': (F.countDistinct(F.col('querystring')))
        }

    def compute(self, df):

        df = df.withColumn(
            self.feature_name,
            F.col('num_unique_queries').cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_total(
            current[cls.feature_name_from_class()],
            past.get(cls.feature_name_from_class())
        )

