# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.util.enums import FeatureComputeType
import numpy as np
from pyspark.sql import functions as F, types as T

from baskerville.features.feature_request_interval_variance import \
    FeatureRequestIntervalVariance, FeatureRequestTotal, \
    FeatureRequestIntervalAverage

from tests.unit.baskerville_tests.helpers.spark_testing_base import \
    FeatureSparkTestCase


class TestSparkRequestIntervalVariance(FeatureSparkTestCase):

    def setUp(self):
        super(TestSparkRequestIntervalVariance, self).setUp()
        self.feature = FeatureRequestIntervalVariance()

    def test_instance(self):
        self.assertTrue(hasattr(self.feature, 'feature_name'))
        self.assertTrue(hasattr(self.feature, 'COLUMNS'))
        self.assertTrue(hasattr(self.feature, 'DEPENDENCIES'))
        self.assertTrue(hasattr(self.feature, 'DEFAULT_VALUE'))
        self.assertTrue(hasattr(self.feature, 'compute_type'))

        self.assertTrue(self.feature.feature_name ==
                        'request_interval_variance')
        self.assertTrue(
            self.feature.columns == ['@timestamp'])
        self.assertTrue(self.feature.dependencies == [
            FeatureRequestTotal, FeatureRequestIntervalAverage])
        self.assertTrue(self.feature.DEFAULT_VALUE == 0.)
        self.assertTrue(self.feature.compute_type ==
                        FeatureComputeType.variance)
        self.assertIsNotNone(self.feature.feature_name)
        self.assertIsNotNone(self.feature.feature_default)

        self.assertTrue(isinstance(self.feature.feature_name, str))
        self.assertTrue(isinstance(self.feature.feature_default, float))

    def test_compute_single_record_no_variance(self):
        ats_record = {
            "client_ip": '55.555.55.55',
            "client_request_host": 'host',
            "@timestamp": '2018-01-17T08:30:00.000Z',
            "content_type": 'application/javascript',
            "client_url": 'page1/page2/page3?query',
        }
        sub_df = self.get_sub_df_for_feature(self.feature, [ats_record])
        result = self.feature.compute(sub_df)
        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(self.feature.feature_default).cast('float')
        )
        expected_df = self.schema_helper(
            expected_df, result.schema, [self.feature.feature_name]
        )

        result.show()
        expected_df.show()

        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_compute_multiple_records_same_interval(self):
        first_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            '@timestamp': '2018-04-17T08:00:00Z',
            'agent': 'ua',
            'content_type': 'application/javascript',
            'request': '/one/two/three/four.png',
        }
        second_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            '@timestamp': '2018-04-17T09:00:00Z',
            'agent': 'ua',
            'content_type': 'application/javascript',
            'request': '/one/two/three.png',
        }
        third_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            '@timestamp': '2018-04-17T10:00:00Z',
            'agent': 'ua',
            'content_type': 'html',
            'request': '/one/two.png',
        }

        sub_df = self.get_sub_df_for_feature(
            self.feature,
            [
                first_ats_record,
                second_ats_record,
                third_ats_record,
            ]
        )
        result = self.feature.compute(sub_df)

        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(self.feature.feature_default).cast('float')
        )
        expected_df = self.schema_helper(
            expected_df, result.schema, [self.feature.feature_name]
        )

        result.show()
        expected_df.show()

        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_compute_multiple_records_vary_interval(self):
        first_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            '@timestamp': '2018-04-17T08:00:00Z',
            'agent': 'ua',
            'content_type': 'application/javascript',
            'request': '/one/two/three/four.png',
        }
        second_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            '@timestamp': '2018-04-17T08:10:00Z',
            'agent': 'ua',
            'content_type': 'application/javascript',
            'request': '/one/two/three.png',
        }
        third_ats_record = {
            "client_ip": '55.555.55.55',
            'client_request_host': 'test',
            '@timestamp': '2018-04-17T08:30:00Z',
            'agent': 'ua',
            'content_type': 'html',
            'request': '/one/two.png',
        }

        sub_df = self.get_sub_df_for_feature(
            self.feature,
            [
                first_ats_record,
                second_ats_record,
                third_ats_record,
            ]
        )
        result = self.feature.compute(sub_df)

        expected_df = sub_df.withColumn(
            self.feature.feature_name,
            F.lit(np.var([10., 20.], ddof=1)).cast('float')
        )
        expected_df = self.schema_helper(
            expected_df, result.schema, [self.feature.feature_name]
        )

        result.show()
        expected_df.show()

        self.assertDataFrameEqual(
            result,
            expected_df
        )

    def test_update_row(self):
        requests = FeatureRequestTotal()
        ave = FeatureRequestIntervalAverage()
        test_current = {self.feature.feature_name: 6.,
                        requests.feature_name: 3.,
                        ave.feature_name: 5.}
        test_past = {self.feature.feature_name: 2.,
                     requests.feature_name: 1.,
                     ave.feature_name: 4.}
        value = self.feature.update_row(
            test_current, test_past
        )

        from baskerville.features.helpers import update_variance
        expected_value = update_variance(2., 6., 1., 3., 4., 5.)

        self.assertAlmostEqual(value, expected_value, places=2)

    def test_update(self):
        count_col = FeatureRequestTotal.feature_name_from_class()
        mean_col = FeatureRequestIntervalAverage.feature_name_from_class()
        schema = T.StructType([
            T.StructField(
                self.feature.current_features_column,
                T.MapType(T.StringType(), T.FloatType())
            ),
            T.StructField(
                self.feature.past_features_column,
                T.MapType(T.StringType(), T.FloatType())
            ),
        ])

        sub_df = self.session.createDataFrame(
            [{
                self.feature.current_features_column: {
                    self.feature.feature_name: 6.,
                    count_col: 3.,
                    mean_col: 5.,
                },
                self.feature.past_features_column: {
                    self.feature.feature_name: 2.,
                    count_col: 1.,
                    mean_col: 4.,
                }
            }],
            schema=schema
        )
        result_df = self.feature.update(
            sub_df
        )

        result_df.show()
        value = result_df.select(
            self.feature.updated_feature_col_name
        ).collect()[0][self.feature.updated_feature_col_name]

        from baskerville.features.helpers import update_variance
        expected_value = update_variance(2., 6., 1., 3., 4., 5.)

        self.assertAlmostEqual(value, expected_value, places=2)
