from baskerville.features.updateable_features import UpdaterRatio
from pyspark.sql import functions as F

from baskerville.features.feature_js_total import FeatureJsTotal
from baskerville.features.feature_html_total import FeatureHtmlTotal
from baskerville.features.helpers import update_ratio


class FeatureJsToHtmlRatio(UpdaterRatio):
    """
    For each IP compute the ratio of JS to HTML requests.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['content_type']
    DEPENDENCIES = [FeatureJsTotal, FeatureHtmlTotal]

    def __init__(self):
        super(FeatureJsToHtmlRatio, self).__init__()
        self.group_by_aggs = {
            'html_count': F.count(F.when(F.col('is_html') == True,  # noqa
                                         F.col('is_html'))),
            'js_count': F.count(F.when(F.col('is_js') == True,  # noqa
                                       F.col('is_js')))
        }
        self.pre_group_by_calcs = {
            'is_html': F.col('content_type') == 'text/html',
            'is_js': F.array_contains(F.split(F.col(
                'content_type'), '/'
            ), 'javascript'),
        }

    def compute(self, df):
        from pyspark.sql import functions as F

        df = df.withColumn(
            self.feature_name,
            F.when(
                F.col('html_count').cast('float') > 0.,
                (F.col('js_count').cast('float') /
                 F.col('html_count').cast('float')).cast('float')
            ).otherwise(F.col('js_count').cast('float') / F.lit(0.01))
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_ratio(
            past.get(FeatureJsTotal.feature_name_from_class()),
            past.get(FeatureHtmlTotal.feature_name_from_class()),
            current[FeatureJsTotal.feature_name_from_class()],
            current[FeatureHtmlTotal.feature_name_from_class()]
        )

    def update(self, df, feat_column='features', old_feat_column='old_features'):
        return super().update(
            df,
            FeatureJsTotal.feature_name_from_class(),
            FeatureHtmlTotal.feature_name_from_class(),
        )
