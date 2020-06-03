from baskerville.features.updateable_features import UpdaterRatio
from baskerville.util.enums import FeatureComputeType
from pyspark.sql import functions as F

from baskerville.features.feature_css_total import FeatureCssTotal
from baskerville.features.feature_html_total import FeatureHtmlTotal
from baskerville.features.helpers import update_ratio


class FeatureCssToHtmlRatio(UpdaterRatio):
    """
    For each IP compute the ratio of CSS to HTML requests.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['content_type']
    DEPENDENCIES = [FeatureCssTotal, FeatureHtmlTotal]
    COMPUTE_TYPE = FeatureComputeType.ratio

    def __init__(self):
        super(FeatureCssToHtmlRatio, self).__init__()
        self.group_by_aggs = {
            'html_count': F.count(F.when(F.col('is_html') == True,  # noqa
                                         F.col('is_html'))),
            'css_count': F.count(F.when(F.col('is_css') == True,  # noqa
                                        F.col('is_css')))
        }
        self.pre_group_by_calcs = {
            'is_html': F.col('content_type') == 'text/html',
            'is_css': F.col('content_type') == 'text/css',
        }

    def compute(self, df):
        from pyspark.sql import functions as F

        df = df.withColumn(
            self.feature_name,
            F.when(
                F.col('html_count') > 0.,
                (F.col('css_count').cast('float') /
                 F.col('html_count').cast('float')).cast('float')
            ).otherwise(
                F.col('css_count').cast('float') / F.lit(0.01)
            )
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_ratio(
            past.get(FeatureCssTotal.feature_name_from_class()),
            past.get(FeatureHtmlTotal.feature_name_from_class()),
            current[FeatureCssTotal.feature_name_from_class()],
            current[FeatureHtmlTotal.feature_name_from_class()]
        )

    def update(self, df, feat_column='features', old_feat_column='old_features'):
        return super().update(
            df,
            FeatureCssTotal.feature_name_from_class(),
            FeatureHtmlTotal.feature_name_from_class(),
        )
