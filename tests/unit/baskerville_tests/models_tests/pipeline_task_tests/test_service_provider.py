# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from unittest import mock

from pyspark.sql import SparkSession

from baskerville.models.config import BaskervilleConfig
from baskerville.models.request_set_cache import RequestSetSparkCache
from tests.unit.baskerville_tests.helpers.spark_testing_base import \
    SQLTestCaseLatestSpark
from tests.unit.baskerville_tests.helpers.utils import test_baskerville_conf


class TestServiceProvider(SQLTestCaseLatestSpark):
    def setUp(self):
        super(TestServiceProvider, self).setUp()
        self.test_conf = test_baskerville_conf
        self.baskerville_config = BaskervilleConfig(self.test_conf).validate()

    def tearDown(self) -> None:
        if self.service_provider:
            self.service_provider.reset()

    def _helper_initialize_service_provider(self):
        from baskerville.models.pipeline_tasks.service_provider import \
            ServiceProvider

        self.service_provider = ServiceProvider(
            self.baskerville_config
        )

    def test_initialize_spark_service(self):
        self._helper_initialize_service_provider()
        self.service_provider.initialize_spark_service()
        self.assertIsInstance(
            self.service_provider.spark, SparkSession
        )
        id_spark = id(self.service_provider.spark)
        self.service_provider.initialize_spark_service()
        self.assertEqual(id_spark, id(self.service_provider.spark))

    def test_initialize_request_set_cache_service(self):
        self._helper_initialize_service_provider()
        self.service_provider.initialize_request_set_cache_service()
        self.assertIsInstance(
            self.service_provider.request_set_cache, RequestSetSparkCache
        )
        id_cache = id(self.service_provider.request_set_cache)
        self.service_provider.initialize_request_set_cache_service()
        self.assertEqual(id_cache, id(self.service_provider.request_set_cache))

    @mock.patch('baskerville.util.baskerville_tools.BaskervilleDBTools')
    def test_initialize_db_tools_service(self, mock_db_tools):
        self._helper_initialize_service_provider()
        self.service_provider.initialize_db_tools_service()
        mock_db_tools.assert_called_once_with(
            self.service_provider.config.database
        )
        mock_db_tools.return_value.connect_to_db.assert_called_once()

    def test_initialize_model_service(self):
        self._helper_initialize_service_provider()
        self.service_provider.initialize_feature_manager_service()
        self.service_provider.initialize_model_service()

    def test_initialize_feature_manager_service(self):
        self._helper_initialize_service_provider()
        self.service_provider.initialize_feature_manager_service()
        # todo: this is affected by what turn the tests are run
        # self.assertIsInstance(
        #     self.service_provider.feature_manager, FeatureManager
        # )

    def test_initalize_ml_services(self):
        self._helper_initialize_service_provider()
        self.service_provider.initialize_feature_manager_service = \
            mock.MagicMock()
        self.service_provider.initialize_model_service = mock.MagicMock()

        self.service_provider.initalize_ml_services()
        self.service_provider.initialize_feature_manager_service.\
            assert_called_once()
        self.service_provider.initialize_model_service.assert_called_once()

    def test_refresh_cache(self):
        """
        Does not test request set cache itself - there are other tests for that
        Just makes sure the proper functions are called
        """
        self._helper_initialize_service_provider()
        df = self.session.createDataFrame([
            {'client_ip': '1', 'client_request_host': '2'}
        ])
        self.service_provider.initialize_request_set_cache_service()
        mock_rs = mock.MagicMock()
        self.service_provider.request_set_cache = mock_rs
        self.service_provider.refresh_cache(df)
        self.service_provider.request_set_cache.\
            update_self.assert_called_once()

    def test_filter_cache(self):
        """
        Does not test request set cache itself - there are other tests for that
        Just makes sure the proper functions are called
        """
        self._helper_initialize_service_provider()
        df = self.session.createDataFrame([
            {'ip': '1', 'target': '2'}
        ])
        self.service_provider.initialize_request_set_cache_service()
        mock_rs = mock.MagicMock()
        self.service_provider.request_set_cache = mock_rs
        self.service_provider.filter_cache(df)
        self.service_provider.request_set_cache.filter_by.assert_called_once()

    def test_add_cache_columns(self):
        self._helper_initialize_service_provider()
        df = self.session.createDataFrame([
            {'ip': '1', 'target': '2'}
        ])
        self.service_provider.initialize_request_set_cache_service()
        df_with_cache_cols = self.service_provider.add_cache_columns(df)
        for col in self.service_provider.cache_columns:
            self.assertIn(col, df_with_cache_cols.columns)

    @mock.patch('baskerville.spark.helpers.reset_spark_storage')
    def test_finish_up(self, mock_reset_spark_storage):
        self._helper_initialize_service_provider()
        self.service_provider.finish_up()
        # todo: depends on the way the tests are run
        # mock_reset_spark_storage.assert_called()

    def test_reset(self):
        self._helper_initialize_service_provider()
        df = self.session.createDataFrame([
            {'client_ip': '1', 'client_request_host': '2'}
        ]).persist()
        df.createTempView('test')
        self.assertTrue(df.is_cached)
        print(self.session.sparkContext._jsc.getPersistentRDDs().items())
        self.service_provider.reset()
        self.assertEqual(
            len(list(
                self.session.sparkContext._jsc.getPersistentRDDs().items()
            )),
            0
        )
        # self.assertFalse(df.is_cached)
        print(self.session.catalog.listDatabases())
