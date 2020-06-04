# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.features.updateable_features import UpdaterRatio
from pyspark.sql import functions as F

from baskerville.features.feature_image_total import FeatureImageTotal
from baskerville.features.feature_html_total import FeatureHtmlTotal
from baskerville.features.helpers import update_ratio


class FeatureImageToHtmlRatio(UpdaterRatio):
    """
    For each IP compute the ratio of image to HTML requests.
    """
    DEFAULT_VALUE = 0.
    COLUMNS = ['content_type']
    DEPENDENCIES = [FeatureImageTotal, FeatureHtmlTotal]

    def __init__(self):
        super(FeatureImageToHtmlRatio, self).__init__()

        self.group_by_aggs = {
            'html_count': F.count(
                F.when(F.col('is_html') == True, F.col('is_html'))  # noqa
            ),
            'image_count': F.count(
                F.when(F.col('is_image') == True, F.col('is_image'))  # noqa
            )
        }
        self.pre_group_by_calcs = {
            'is_html': F.col('content_type') == 'text/html',
            'is_image': F.array_contains(
                F.split(F.col('content_type'), '/'),
                'image')
        }

    def compute(self, df):
        df = df.withColumn(
            self.feature_name,
            F.when(
                F.col('html_count').cast('float') > 0.,
                (F.col('image_count').cast('float') /
                 F.col('html_count').cast('float')).cast('float')
            ).otherwise(F.col('image_count').cast('float') / F.lit(0.01))
        ).fillna({self.feature_name: self.feature_default})

        return df

    @classmethod
    def update_row(cls, current, past, *args, **kwargs):
        return update_ratio(
            past.get(FeatureImageTotal.feature_name_from_class()),
            past.get(FeatureHtmlTotal.feature_name_from_class()),
            current[FeatureImageTotal.feature_name_from_class()],
            current[FeatureHtmlTotal.feature_name_from_class()]
        )

    def update(self, df, feat_column='features', old_feat_column='old_features'):
        return super().update(
            df,
            FeatureImageTotal.feature_name_from_class(),
            FeatureHtmlTotal.feature_name_from_class(),
        )
