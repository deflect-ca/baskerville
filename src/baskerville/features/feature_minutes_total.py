from baskerville.features.updateable_features import UpdaterReplace
from pyspark.sql import functions as F

from baskerville.features.base_feature import TimeBasedFeature
from baskerville.features.helpers import update_replace


class FeatureMinutesTotal(TimeBasedFeature, UpdaterReplace):
    """
    For each IP compute total length of the request_set (minutes).
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['@timestamp']
    DEPENDENCIES = []

    def __init__(self):
        super(FeatureMinutesTotal, self).__init__()

    def compute(self, df):
        df = df.withColumn(
            self.feature_name,
            F.col('dt').cast('float')
        ).fillna({self.feature_name: self.feature_default})
        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_replace(
            current[cls.feature_name_from_class()]
        )
