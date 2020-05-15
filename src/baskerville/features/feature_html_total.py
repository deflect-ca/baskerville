from baskerville.features.updateable_features import UpdaterTotal
from pyspark.sql import functions as F

from baskerville.features.base_feature import BaseFeature
from baskerville.features.helpers import update_total


class FeatureHtmlTotal(UpdaterTotal):
    """
    For each IP compute the total HTML requests.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['content_type']
    DEPENDENCIES = []

    def __init__(self):
        super(FeatureHtmlTotal, self).__init__()

        self.group_by_aggs = {
            'html_count':  F.count(F.when(F.col('is_html') == True,  # noqa
                                          F.col('is_html')))
        }
        self.pre_group_by_calcs = {
            'is_html': F.col('content_type') == 'text/html'
        }

    def compute(self, df):
        from pyspark.sql import functions as F

        df = df.withColumn(
            self.feature_name,
            F.col('html_count').cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_total(
                    past.get(cls.feature_name_from_class()),
                    current[cls.feature_name_from_class()]
                )
