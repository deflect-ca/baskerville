from baskerville.features.updateable_features import UpdaterTotal
from pyspark.sql import functions as F

from baskerville.features.helpers import update_total


class FeatureCssTotal(UpdaterTotal):
    """
    For each IP compute the total number of CSS.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['content_type']
    DEPENDENCIES = []

    def __init__(self):
        super(FeatureCssTotal, self).__init__()
        self.group_by_aggs = {
            'css_count': F.count(F.when(F.col('is_css') == True,  # noqa
                                        F.col('is_css')))
        }
        self.pre_group_by_calcs = {
            'is_css': F.col('content_type') == 'text/css',
        }

    def compute(self, df):
        from pyspark.sql import functions as F

        df = df.withColumn(
            self.feature_name,
            F.col('css_count').cast('float')
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_total(
            past.get(cls.feature_name_from_class()),
            current[cls.feature_name_from_class()]
        )
