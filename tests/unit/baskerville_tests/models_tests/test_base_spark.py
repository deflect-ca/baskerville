import importlib
import os
import pickle
import baskerville

from datetime import datetime, timedelta
from unittest import mock

from baskerville.db.models import RequestSet
from baskerville.spark.helpers import StorageLevelFactory
from baskerville.util.helpers import get_default_data_path

from tests.unit.baskerville_tests.helpers.spark_testing_base import \
    SQLTestCaseLatestSpark

from pyspark.sql import functions as F

from baskerville.models.config import (
    Config, DatabaseConfig, SparkConfig, DataParsingConfig
)
from baskerville.util.enums import Step, LabelEnum
from pyspark.sql.types import (StructType, StructField, IntegerType,
                               StringType, TimestampType)


class TestSparkPipelineBase(SQLTestCaseLatestSpark):
    def setUp(self):
        super(TestSparkPipelineBase, self).setUp()
        self.db_tools_patcher = mock.patch(
            'baskerville.models.base_spark.BaskervilleDBTools'
        )
        self.spark_patcher = mock.patch(
            'baskerville.models.base_spark.SparkPipelineBase.'
            'instantiate_spark_session'
        )
        self.request_set_cache_patcher = mock.patch(
            'baskerville.models.base_spark.SparkPipelineBase.set_up_request_set_cache'
        )
        self.mock_baskerville_tools = self.db_tools_patcher.start()
        self.mock_spark_session = self.spark_patcher.start()
        self.mock_request_set_cache = self.request_set_cache_patcher.start()
        self.dummy_conf = Config({})
        self.db_conf = DatabaseConfig(
            {'user': 'postgres', 'password': '***', 'host': 'localhost'})
        self.engine_conf = mock.MagicMock()
        self.engine_conf.log_level = 'DEBUG'
        self.engine_conf.time_bucket = 10
        self.engine_conf.data_config.timestamp_column = '@timestamp'
        self.engine_conf.logpath = f'{os.path.dirname(baskerville.__file__)}' \
                                   f'/logs/test_base.log'
        self.engine_conf.metrics = False
        self.engine_conf.cache_expire_time = 10
        self.spark_conf = SparkConfig({'db_driver': 'test'})
        self.spark_conf.validate()

        from baskerville.models.base_spark import SparkPipelineBase
        self.spark_pipeline = SparkPipelineBase(
            self.db_conf,
            self.engine_conf,
            self.spark_conf
        )
        self.spark_pipeline.spark = self.session

    def tearDown(self):
        if mock._is_started(self.db_tools_patcher):
            self.db_tools_patcher.stop()
            self.mock_baskerville_tools.reset_mock()
        if mock._is_started(self.spark_patcher):
            self.spark_patcher.stop()
            self.mock_spark_session.reset_mock()
        if mock._is_started(self.request_set_cache_patcher):
            self.request_set_cache_patcher.stop()
            self.mock_request_set_cache.reset_mock()

    def test_instance(self):
        self.assertTrue(hasattr(self.spark_pipeline, 'db_conf'))
        self.assertTrue(hasattr(self.spark_pipeline, 'clean_up'))
        self.assertTrue(hasattr(self.spark_pipeline, 'group_by_cols'))
        self.assertTrue(
            hasattr(self.spark_pipeline.feature_manager, 'active_features'))
        self.assertTrue(
            hasattr(self.spark_pipeline.feature_manager, 'active_feature_names'))
        self.assertTrue(
            hasattr(self.spark_pipeline.feature_manager, 'active_columns'))
        self.assertTrue(hasattr(self.spark_pipeline, 'logs_df'))
        self.assertTrue(hasattr(self.spark_pipeline, 'request_set_cache'))

        self.assertTrue(
            Step.preprocessing in self.spark_pipeline.step_to_action)
        self.assertTrue(Step.group_by in self.spark_pipeline.step_to_action)
        self.assertTrue(
            Step.feature_calculation in self.spark_pipeline.step_to_action)
        self.assertTrue(
            Step.label_or_predict in self.spark_pipeline.step_to_action)
        self.assertTrue(Step.save in self.spark_pipeline.step_to_action)

    @mock.patch('baskerville.spark.get_or_create_spark_session')
    def test_instantiate_spark_session(self, mock_get_or_create_spark_session):
        self.spark_patcher.stop()
        mock_get_or_create_spark_session.return_value = 'This should be the ' \
                                                        'spark instance'
        importlib.reload(baskerville.models.base_spark)
        from baskerville.models.base_spark import SparkPipelineBase
        spark = SparkPipelineBase.instantiate_spark_session(
            self.spark_pipeline)
        self.assertTrue(spark is not None)
        self.assertEqual(spark, 'This should be the spark instance')
        mock_get_or_create_spark_session.assert_called_once_with(
            self.spark_pipeline.spark_conf
        )
        self.db_tools_patcher.start()

    @mock.patch('baskerville.models.base_spark.instantiate_from_str')
    @mock.patch('baskerville.models.base_spark.bytes')
    def test_initialize(self, mock_bytes, mock_instantiate_from_str):
        # to call get_ml_model_from_db
        self.engine_conf.model_id = -1
        model_index = mock.MagicMock()
        model_index.id = 1
        db_tools = self.mock_baskerville_tools.return_value
        db_tools.get_ml_model_from_db.return_value = model_index

        self.spark_pipeline.initialize()
        self.assertEqual(
            self.spark_pipeline.time_bucket.sec,
            self.engine_conf.time_bucket
        )
        self.assertEqual(
            self.spark_pipeline.time_bucket.td,
            timedelta(seconds=self.engine_conf.time_bucket)
        )

        self.mock_baskerville_tools.assert_called_once_with(
            self.spark_pipeline.db_conf
        )

        db_tools.connect_to_db.assert_called_once()

        self.spark_pipeline.instantiate_spark_session.assert_called_once()
        self.spark_pipeline.set_up_request_set_cache.assert_called_once()

        self.assertEqual(len(self.spark_pipeline.group_by_aggs), 3)
        self.assertTrue('first_request' in self.spark_pipeline.group_by_aggs)
        self.assertTrue('last_request' in self.spark_pipeline.group_by_aggs)
        self.assertTrue('num_requests' in self.spark_pipeline.group_by_aggs)
        self.assertEqual(
            str(self.spark_pipeline.group_by_aggs['first_request']._jc),
            'min(@timestamp) AS `first_request`'
        )
        self.assertEqual(
            str(self.spark_pipeline.group_by_aggs['last_request']._jc),
            'max(@timestamp) AS `last_request`'
        )
        self.assertEqual(
            str(self.spark_pipeline.group_by_aggs['num_requests']._jc),
            'count(@timestamp) AS `num_requests`'
        )

        self.assertEqual(
            len(self.spark_pipeline.feature_manager.column_renamings), 0)
        self.assertEqual(
            len(self.spark_pipeline.feature_manager.active_features), 0)
        self.assertEqual(
            len(self.spark_pipeline.feature_manager.active_feature_names), 0)
        self.assertEqual(
            len(self.spark_pipeline.feature_manager.active_columns), 0)
        self.assertEqual(len(self.spark_pipeline.columns_to_filter_by), 3)
        self.assertSetEqual(
            self.spark_pipeline.columns_to_filter_by,
            {'client_request_host', 'client_ip', '@timestamp'}
        )
        mock_bytes.decode.assert_called_once()
        mock_instantiate_from_str.assert_called_once()

    # def test_initialize_model_path(self):
    #
    #     # to call get_ml_model_from_file
    #     self.engine_conf.model_id = None
    #     self.engine_conf.model_path = 'some test path'
    #     self.spark_pipeline.model_manager.set_anomaly_detector_broadcast = mock.MagicMock()
    #     self.spark_pipeline.initialize()
    #     self.assertEqual(
    #         self.spark_pipeline.time_bucket.sec,
    #         self.engine_conf.time_bucket
    #     )
    #     self.assertEqual(
    #         self.spark_pipeline.time_bucket.td,
    #         timedelta(seconds=self.engine_conf.time_bucket)
    #     )
    #
    #     db_tools = self.spark_pipeline.tools
    #     db_tools.connect_to_db.assert_called_once()
    #
    #     self.spark_pipeline.instantiate_spark_session.assert_called_once()
    #     self.spark_pipeline.set_up_request_set_cache.assert_called_once()
    #
    #     self.assertEqual(len(self.spark_pipeline.group_by_aggs), 3)
    #     self.assertTrue('first_request' in self.spark_pipeline.group_by_aggs)
    #     self.assertTrue('last_request' in self.spark_pipeline.group_by_aggs)
    #     self.assertTrue('num_requests' in self.spark_pipeline.group_by_aggs)
    #     self.assertEqual(
    #         str(self.spark_pipeline.group_by_aggs['first_request']._jc),
    #         'min(@timestamp) AS `first_request`'
    #     )
    #     self.assertEqual(
    #         str(self.spark_pipeline.group_by_aggs['last_request']._jc),
    #         'max(@timestamp) AS `last_request`'
    #     )
    #     self.assertEqual(
    #         str(self.spark_pipeline.group_by_aggs['num_requests']._jc),
    #         'count(@timestamp) AS `num_requests`'
    #     )
    #
    #     self.assertEqual(len(self.spark_pipeline.feature_manager.column_renamings), 0)
    #     self.assertEqual(len(self.spark_pipeline.feature_manager.active_features), 0)
    #     self.assertEqual(len(self.spark_pipeline.feature_manager.active_feature_names), 0)
    #     self.assertEqual(len(self.spark_pipeline.feature_manager.active_columns), 0)
    #     self.assertEqual(len(self.spark_pipeline.columns_to_filter_by), 3)
    #     self.assertSetEqual(
    #         self.spark_pipeline.columns_to_filter_by,
    #         {'client_request_host', 'client_ip', '@timestamp'}
    #     )
    #     db_tools = self.spark_pipeline.tools
    #     db_tools.get_ml_model_from_file.assert_called_once_with(
    #         self.engine_conf.model_path
    #     )

    @mock.patch('baskerville.models.base_spark.instantiate_from_str')
    def test_initialize_no_model_register_metrics(self, mock_instantiate_from_str):

        # for an empty model:
        self.engine_conf.model_id = None
        self.engine_conf.model_path = None
        # set config and mock register metrics:
        self.engine_conf.metrics = mock.MagicMock()
        self.engine_conf.metrics.progress = True
        self.spark_pipeline.register_metrics = mock.MagicMock()
        self.spark_pipeline.initialize()
        self.assertEqual(
            self.spark_pipeline.time_bucket.sec,
            self.engine_conf.time_bucket
        )
        self.assertEqual(
            self.spark_pipeline.time_bucket.td,
            timedelta(seconds=self.engine_conf.time_bucket)
        )

        db_tools = self.spark_pipeline.tools
        db_tools.connect_to_db.assert_called_once()

        self.spark_pipeline.instantiate_spark_session.assert_called_once()
        self.spark_pipeline.set_up_request_set_cache.assert_called_once()

        self.assertEqual(len(self.spark_pipeline.group_by_aggs), 3)
        self.assertTrue('first_request' in self.spark_pipeline.group_by_aggs)
        self.assertTrue('last_request' in self.spark_pipeline.group_by_aggs)
        self.assertTrue('num_requests' in self.spark_pipeline.group_by_aggs)
        self.assertEqual(
            str(self.spark_pipeline.group_by_aggs['first_request']._jc),
            'min(@timestamp) AS `first_request`'
        )
        self.assertEqual(
            str(self.spark_pipeline.group_by_aggs['last_request']._jc),
            'max(@timestamp) AS `last_request`'
        )
        self.assertEqual(
            str(self.spark_pipeline.group_by_aggs['num_requests']._jc),
            'count(@timestamp) AS `num_requests`'
        )

        self.assertEqual(
            len(self.spark_pipeline.feature_manager.column_renamings), 0)
        self.assertEqual(
            len(self.spark_pipeline.feature_manager.active_features), 0)
        self.assertEqual(
            len(self.spark_pipeline.feature_manager.active_feature_names), 0)
        self.assertEqual(
            len(self.spark_pipeline.feature_manager.active_columns), 0)
        self.assertEqual(len(self.spark_pipeline.columns_to_filter_by), 3)
        self.assertSetEqual(
            self.spark_pipeline.columns_to_filter_by,
            {'client_request_host', 'client_ip', '@timestamp'}
        )

    def test_add_calc_columns(self):
        mock_feature = mock.MagicMock()
        now = datetime.utcnow()
        mock_feature.pre_group_by_calcs = {
            '1': F.col('@timestamp'),
            '2': F.col('test'),
        }
        self.spark_pipeline.feature_manager.pre_group_by_calculations = \
            mock_feature.pre_group_by_calcs
        self.spark_pipeline.feature_manager.active_features = [mock_feature]
        logs = [
            {'@timestamp': now, 'test': 'a'},
            {'@timestamp': now, 'test': 'b'},
            {'@timestamp': now + timedelta(seconds=1), 'test': 'c'},
        ]
        df = self.session.createDataFrame(logs)
        mock_feature.misc_compute = (lambda df: df)

        self.spark_pipeline.logs_df = df
        self.spark_pipeline.add_calc_columns()

        self.assertTrue('1' in self.spark_pipeline.logs_df.columns)
        self.assertTrue('2' in self.spark_pipeline.logs_df.columns)
        self.assertTrue('@timestamp' in self.spark_pipeline.logs_df.columns)
        self.assertTrue('test' in self.spark_pipeline.logs_df.columns)

        df_expected = df.withColumn('1', F.col('@timestamp'))
        df_expected = df_expected.withColumn('2', F.col('test'))

        df_expected = self.fix_schema(
            df_expected,
            self.spark_pipeline.logs_df.schema,
            fields=self.spark_pipeline.logs_df.columns
        )

        self.assertDataFrameEqual(self.spark_pipeline.logs_df, df_expected)

    @mock.patch('baskerville.models.base_spark.instantiate_from_str')
    @mock.patch('baskerville.models.base_spark.bytes')
    def test_add_post_group_columns(self, mock_bytes, mock_instantiate_from_str):
        # spark saves binary as byte array
        pickled = bytearray(pickle.dumps(-1))

        logs = [
            {
                'client_request_host': 'testhost',
                'client_ip': '1',
                'ip': '1',
                'id_runtime': -1,
                'time_bucket': 10,
                'old_subset_count': 10,
                'target': 'testhost',
                'first_ever_request': datetime(2018, 1, 1, 11, 30, 10),
                'first_request': datetime(2018, 1, 1, 11, 30, 10),
                'last_request': datetime(2018, 1, 1, 11, 50, 10),
            }, {
                'client_request_host': 'testhost',
                'client_ip': '1',
                'ip': '1',
                'id_runtime': -1,
                'time_bucket': 10,
                'old_subset_count': 10,
                'target': 'testhost',
                'first_ever_request': datetime(2018, 1, 1, 12, 40, 10),
                'first_request': datetime(2018, 1, 1, 12, 40, 10),
                'last_request': datetime(2018, 1, 1, 12, 50, 10),
            }
        ]
        schema = StructType([
            StructField('client_request_host', StringType(), True),
            StructField('client_ip', StringType(), True),
            StructField('ip', StringType(), True),
            StructField('id_runtime', IntegerType(), False),
            StructField('time_bucket', IntegerType(), False),
            StructField('old_subset_count', IntegerType(), False),
            StructField('target', StringType(), True),
            StructField('first_ever_request', TimestampType(), True),
            StructField('first_request', TimestampType(), True),
            StructField('last_request', TimestampType(), True),
        ])
        self.spark_pipeline.runtime = mock.MagicMock()
        self.spark_pipeline.runtime.id = -1

        model_index = mock.MagicMock()
        model_index.id = 1
        db_tools = self.mock_baskerville_tools.return_value
        db_tools.get_ml_model_from_db.return_value = model_index

        self.spark_pipeline.initialize()

        df = self.session.createDataFrame(logs, schema=schema)

        self.spark_pipeline.logs_df = df.select(
            'client_request_host',
            'client_ip',
            'first_ever_request',
            'first_request',
            'last_request',
            'old_subset_count',
        )

        df = df.withColumn('classifier', F.lit(pickled))
        df = df.withColumn('model', F.lit(pickled))
        df = df.withColumn('scaler', F.lit(pickled))
        df = df.withColumn('model_features', F.lit(pickled))
        df = df.withColumn('subset_count', F.lit(10))
        df = df.withColumn('model_version', F.lit(1))
        df = df.withColumn('start', F.col('first_ever_request'))
        df = df.withColumn('stop', F.col('last_request'))

        self.spark_pipeline.model = None
        self.spark_pipeline.engine_conf.time_bucket = 600
        self.spark_pipeline.add_cache_columns = mock.MagicMock()
        self.spark_pipeline.add_post_groupby_columns()

        self.assertTrue('id_runtime' in self.spark_pipeline.logs_df.columns)
        self.assertTrue('ip' in self.spark_pipeline.logs_df.columns)
        self.assertTrue('target' in self.spark_pipeline.logs_df.columns)

        columns = self.spark_pipeline.logs_df.columns

        self.spark_pipeline.logs_df.select(columns).show()
        df = self.fix_schema(df.select(columns),
                             self.spark_pipeline.logs_df.schema)
        df.show()

        self.assertTrue(df.schema == self.spark_pipeline.logs_df.schema)
        self.assertTrue(df.count() == self.spark_pipeline.logs_df.count())

        self.assertDataFrameEqual(
            self.spark_pipeline.logs_df,
            df
        )

    @mock.patch('baskerville.models.base_spark.instantiate_from_str')
    @mock.patch('baskerville.models.base_spark.bytes')
    def test_add_post_group_columns_no_ml_model(self, mock_bytes, mock_instantiate_from_str):
        logs = [
            {
                'client_request_host': 'testhost',
                'client_ip': '1',
                'ip': '1',
                'id_runtime': -1,
                'time_bucket': 10,
                'old_subset_count': 10,
                'target': 'testhost',
                'first_ever_request': datetime(2018, 1, 1, 11, 30, 10),
                'first_request': datetime(2018, 1, 1, 11, 30, 10),
                'last_request': datetime(2018, 1, 1, 11, 50, 10),
            }, {
                'client_request_host': 'testhost',
                'client_ip': '1',
                'ip': '1',
                'id_runtime': -1,
                'time_bucket': 10,
                'old_subset_count': 10,
                'target': 'testhost',
                'first_ever_request': datetime(2018, 1, 1, 12, 40, 10),
                'first_request': datetime(2018, 1, 1, 12, 40, 10),
                'last_request': datetime(2018, 1, 1, 12, 50, 10),
            }
        ]
        schema = StructType([
            StructField('client_request_host', StringType(), True),
            StructField('client_ip', StringType(), True),
            StructField('ip', StringType(), True),
            StructField('id_runtime', IntegerType(), False),
            StructField('time_bucket', IntegerType(), False),
            StructField('old_subset_count', IntegerType(), False),
            StructField('target', StringType(), True),
            StructField('first_ever_request', TimestampType(), True),
            StructField('first_request', TimestampType(), True),
            StructField('last_request', TimestampType(), True),
        ])

        model_index = mock.MagicMock()
        model_index.id = 1
        db_tools = self.mock_baskerville_tools.return_value
        db_tools.get_ml_model_from_db.return_value = model_index

        self.spark_pipeline.initialize()

        self.spark_pipeline.runtime = mock.MagicMock()
        self.spark_pipeline.runtime.id = -1
        df = self.session.createDataFrame(logs, schema=schema)

        self.spark_pipeline.logs_df = df.select(
            'client_request_host',
            'client_ip',
            'first_ever_request',
            'first_request',
            'last_request',
            'old_subset_count',
        )
        df = df.withColumn(
            'model_version', F.lit(
                1
            )
        )
        df = df.withColumn('subset_count', F.lit(10))
        df = df.withColumn('start', F.col('first_ever_request'))
        df = df.withColumn('stop', F.col('last_request'))

        self.spark_pipeline.engine_conf.time_bucket = 600
        self.spark_pipeline.add_cache_columns = mock.MagicMock()
        self.spark_pipeline.add_post_groupby_columns()

        self.assertTrue('id_runtime' in self.spark_pipeline.logs_df.columns)
        self.assertTrue('ip' in self.spark_pipeline.logs_df.columns)
        self.assertTrue('target' in self.spark_pipeline.logs_df.columns)

        columns = self.spark_pipeline.logs_df.columns

        self.spark_pipeline.logs_df.select(columns).show()
        df = self.fix_schema(df.select(columns),
                             self.spark_pipeline.logs_df.schema)
        df.show()

        self.assertTrue(df.schema == self.spark_pipeline.logs_df.schema)
        self.assertTrue(df.count() == self.spark_pipeline.logs_df.count())

        self.assertDataFrameEqual(
            self.spark_pipeline.logs_df,
            df
        )

    @mock.patch('baskerville.models.base_spark.instantiate_from_str')
    @mock.patch('baskerville.models.base_spark.bytes')
    def test_group_by(self, mock_bytes, mock_instantiate_from_str):
        logs = [
            {
                'client_request_host': 'testhost',
                'client_ip': '1',
                'ip': '1',
                'id_runtime': -1,
                'target': 'testhost',
                '@timestamp': datetime(2018, 1, 1, 10, 30, 10),
            }, {
                'client_request_host': 'testhost',
                'client_ip': '1',
                'ip': '1',
                'id_runtime': -1,
                'target': 'testhost',
                '@timestamp': datetime(2018, 1, 1, 12, 30, 10),
            },
            {
                'client_request_host': 'other testhost',
                'client_ip': '1',
                'ip': '1',
                'id_runtime': -1,
                'target': 'testhost',
                '@timestamp': datetime(2018, 1, 1, 12, 30, 10),
            }
        ]
        df = self.session.createDataFrame(logs)
        self.spark_pipeline.logs_df = df

        mock_feature = mock.MagicMock()
        mock_feature.group_by_aggs = {'1': F.collect_set(F.col('client_ip'))}
        mock_feature.columns = []

        self.spark_pipeline.feature_manager.get_active_features = mock.MagicMock()
        self.spark_pipeline.feature_manager.get_active_features.return_value = [
            mock_feature]
        self.spark_pipeline.add_post_groupby_columns = mock.MagicMock()
        self.spark_pipeline.feature_manager.active_features = [mock_feature]
        self.spark_pipeline.model = None
        self.spark_pipeline.active_columns = self.spark_pipeline.feature_manager.get_active_columns()

        model_index = mock.MagicMock()
        model_index.id = 1
        db_tools = self.mock_baskerville_tools.return_value
        db_tools.get_ml_model_from_db.return_value = model_index

        self.spark_pipeline.initialize()

        self.spark_pipeline.group_by()
        grouped_df = df.groupBy('client_request_host', 'client_ip').agg(
            F.min(F.col('@timestamp')).alias('first_request'),
            F.max(F.col('@timestamp')).alias('last_request'),
            F.count(F.col('@timestamp')).alias('num_requests'),
            F.collect_set(F.col('client_ip')).alias('1')
        )

        self.assertEqual(self.spark_pipeline.logs_df.count(), 2)
        self.assertTrue('1' in self.spark_pipeline.logs_df.columns)
        self.assertTrue('first_request' in self.spark_pipeline.logs_df.columns)
        self.assertTrue('last_request' in self.spark_pipeline.logs_df.columns)
        self.assertTrue('num_requests' in self.spark_pipeline.logs_df.columns)
        self.assertDataFrameEqual(grouped_df, self.spark_pipeline.logs_df)

    @mock.patch('baskerville.models.base_spark.instantiate_from_str')
    @mock.patch('baskerville.models.base_spark.bytes')
    def test_group_by_empty_feature_group_by_aggs(self, mock_bytes, mock_instantiate_from_str):
        logs = [
            {
                'client_request_host': 'testhost',
                'client_ip': '1',
                'ip': '1',
                'id_incident': -1,
                'target': 'testhost',
                '@timestamp': datetime(2018, 1, 1, 10, 30, 10),
            }, {
                'client_request_host': 'testhost',
                'client_ip': '1',
                'ip': '1',
                'id_incident': -1,
                'target': 'testhost',
                '@timestamp': datetime(2018, 1, 1, 12, 30, 10),
            },
            {
                'client_request_host': 'other testhost',
                'client_ip': '1',
                'ip': '1',
                'id_incident': -1,
                'target': 'testhost',
                '@timestamp': datetime(2018, 1, 1, 12, 30, 10),
            }
        ]

        df = self.session.createDataFrame(logs)
        self.spark_pipeline.logs_df = df

        mock_feature = mock.MagicMock()
        mock_feature.columns = []
        self.spark_pipeline.add_post_groupby_columns = mock.MagicMock()
        self.spark_pipeline.set_broadcasts = mock.MagicMock()
        self.spark_pipeline.feature_manager.active_features = [mock_feature]

        model_index = mock.MagicMock()
        model_index.id = 1
        db_tools = self.mock_baskerville_tools.return_value
        db_tools.get_ml_model_from_db.return_value = model_index

        self.spark_pipeline.initialize()

        self.spark_pipeline.group_by()
        grouped_df = df.groupBy('client_request_host', 'client_ip').agg(
            F.min(F.col('@timestamp')).alias('first_request'),
            F.max(F.col('@timestamp')).alias('last_request'),
            F.count(F.col('@timestamp')).alias('num_requests'),
        )

        self.assertEqual(self.spark_pipeline.logs_df.count(), 2)
        self.assertTrue('num_requests' in self.spark_pipeline.logs_df.columns)
        self.assertTrue('first_request' in self.spark_pipeline.logs_df.columns)
        self.assertTrue('last_request' in self.spark_pipeline.logs_df.columns)
        self.assertDataFrameEqual(grouped_df, self.spark_pipeline.logs_df)

    def test_features_to_dict(self):
        logs = [
            {
                'feature1': 0.4,
                'feature2': 1.,
                'feature3': 1.,
            }, {
                'feature1': 0.2,
                'feature2': 1.,
                'feature3': 1.,
            }
        ]
        mock_feature1 = mock.MagicMock()
        mock_feature2 = mock.MagicMock()
        mock_feature3 = mock.MagicMock()

        mock_feature1.feature_name = 'feature1'
        mock_feature2.feature_name = 'feature2'
        mock_feature3.feature_name = 'feature3'

        for i, each in enumerate(logs):
            each.update({'features': each})
            logs[i] = each

        df = self.session.createDataFrame(logs)
        self.spark_pipeline.logs_df = df.select(df.columns[:-1])
        self.spark_pipeline.feature_manager.active_feature_names = self.spark_pipeline.logs_df.columns
        self.spark_pipeline.feature_manager.active_features = [
            mock_feature1, mock_feature2, mock_feature3
        ]

        self.spark_pipeline.features_to_dict('features')
        self.spark_pipeline.logs_df.show()
        df.show()

        # we've got a rounding issue. e.g.: df contains:
        # Row(feature1=0.4, feature2=1.0, feature3=1.0,
        #   features={'feature2': 1.0,
        #             'features': None,
        #             'feature3': 1.0,
        #             'feature1': 0.4}),
        # and logs df contain:
        # Row(feature1=0.4, feature2=1.0, feature3=1.0,
        #    features={'feature2': 1.0,
        #              'feature3': 1.0,
        #              'feature1': 0.4000000059604645}),
        # so we collect and compare dicts instead
        # Another solution - more general - would be to change floats to
        # decimals and set the precision to e.g 6.
        # from decimal import *
        # getcontext().prec =  6
        # df = spark.createDataFrame([{'a': Decimal(2)}])
        # # also:
        # current_context = Context(prec=6, rounding=ROUND_UP)
        # setcontext(current_context)
        # # not sure how this will play out in spark workers
        cdf = df.sort(df.columns[:-1]).collect()
        cldf = self.spark_pipeline.logs_df.sort(df.columns[:-1]).collect()

        for i, row in enumerate(cdf):
            for k, v in cldf[i].features.items():
                self.assertAlmostEqual(getattr(row, k), v, 1)

    @mock.patch('baskerville.models.base_spark.instantiate_from_str')
    @mock.patch('baskerville.models.base_spark.bytes')
    def test_save(self, mock_bytes, mock_instantiate_from_str):
        now = datetime.now()

        logs = [
            {
                'id': None,
                'client_request_host': 'testhost',
                'client_ip': '1',
                'ip': '1',
                'id_request_set': -1,
                'id_attribute': '',
                'id_runtime': -1,
                'request_set_prediction': -1,
                'prediction': -1,
                'num_requests': 10,
                'label': -1,
                'request_set_length': 2,
                'subset_count': 2,
                'target': 'testhost',
                'start': datetime(2018, 1, 1, 10, 30, 10),
                'stop': datetime(2018, 1, 1, 12, 45, 10),
                'last_request': datetime(2018, 1, 1, 12, 45, 10),
                'total_seconds': 50,
                'features': {'f1': 0.2},
                'updated_at': now,
                'r': 0.,
                'time_bucket': 10,
                'model_version': 'test',
            },
            {
                'id': 1,
                'client_request_host': 'other testhost',
                'client_ip': '1',
                'ip': '1',
                'id_request_set': None,
                'id_attribute': '',
                'id_runtime': -1,
                'request_set_prediction': -1,
                'prediction': -1,
                'score': 0.0,
                'num_requests': 2,
                'request_set_length': 1,
                'subset_count': 1,
                'target': 'other testhost',
                'label': -1,
                'start': datetime(2018, 1, 1, 12, 30, 10),
                'stop': datetime(2018, 1, 1, 12, 40, 10),
                'last_request': datetime(2018, 1, 1, 12, 40, 10),
                'total_seconds': 50,
                'features': {'f1': 0.1},
                'updated_at': now,
                'time_bucket': 10,
                'model_version': 'test',
            }
        ]
        self.spark_pipeline.set_broadcasts = mock.MagicMock()
        self.spark_pipeline.initialize()

        df = self.session.createDataFrame(logs)
        self.spark_pipeline.logs_df = df
        self.spark_pipeline.spark = mock.MagicMock()
        self.spark_pipeline.runtime = mock.MagicMock()
        self.spark_pipeline.runtime.id = 1
        self.spark_pipeline.save_df_to_table = mock.MagicMock()
        self.spark_pipeline.update_database = mock.MagicMock()
        self.spark_pipeline.refresh_cache = mock.MagicMock()

        self.spark_pipeline.save()

        self.spark_pipeline.save_df_to_table.assert_called()

        request_sets_df = df.select(
            RequestSet.columns
        ).withColumnRenamed(
            'id_request_set', 'id'
        ).withColumnRenamed(
            'request_set_prediction', 'prediction'
        )
        # postgres doesn't want the id in the insert
        request_sets_df = request_sets_df.drop('id')

        # # dfs are no longer accessible
        # columns = [c for c in request_sets_df.columns if c != 'features']
        # self.assertDataFrameEqual(
        #     self.spark_pipeline.request_sets_already_in_db.select(columns),
        #     request_sets_df.select(columns).where(F.col('id').isNotNull())
        # )

        actual_call = self.spark_pipeline.save_df_to_table.call_args_list[0][0]
        self.assertListEqual(actual_call[0].columns, request_sets_df.columns)

        self.spark_pipeline.refresh_cache.assert_called_once()

    @mock.patch('baskerville.models.base_spark.F.udf')
    def test_predict_no_ml_model(self, mock_udf):
        logs = [
            {
                'ip': '1',
                'target': 'testhost',
                'afeature': 0.1,
            },
            {
                'ip': '1',
                'target': 'testhost',
                'afeature': 1.,
            }
        ]
        mock_udf_predict_dict = mock_udf.return_value

        mock_udf_predict_dict.return_value = F.struct(
            [
                F.lit(0.).cast('float').alias('prediction'),
                F.lit(1.).cast('float').alias('score'),
                F.lit(1.).cast('float').alias('threshold')
            ]
        )
        df = self.session.createDataFrame(logs[1:])

        # set cache
        self.spark_pipeline.request_sets_df = df
        self.spark_pipeline.logs_df = df.withColumn(
            'features', F.lit(0)
        )
        self.spark_pipeline.predict()

        df = df.withColumn('features', F.lit(0))
        df = df.withColumn('prediction', F.lit(LabelEnum.unknown.value).cast('int'))
        df = df.withColumn('score', F.lit(LabelEnum.unknown.value).cast('float'))
        df = self.fix_schema(
            df,
            self.spark_pipeline.logs_df.schema,
            self.spark_pipeline.logs_df.columns
        )

        self.assertDataFrameEqual(df, self.spark_pipeline.logs_df)

    def test_feature_extraction(self):
        mock_feature1 = mock.MagicMock()
        mock_feature2 = mock.MagicMock()
        mock_feature3 = mock.MagicMock()
        mock_feature4 = mock.MagicMock()
        self.spark_pipeline.feature_manager.active_features = [
            mock_feature1,
            mock_feature2,
            mock_feature3,
            mock_feature4,
        ]

        self.spark_pipeline.feature_extraction()
        mock_feature1.compute.assert_called_once_with(None)
        mock_feature2.compute.assert_called_once_with(
            mock_feature1.compute.return_value
        )
        mock_feature3.compute.assert_called_once_with(
            mock_feature2.compute.return_value
        )
        mock_feature4.compute.assert_called_once_with(
            mock_feature3.compute.return_value
        )

    @mock.patch('baskerville.spark.helpers.col_to_json')
    def test_save_df_to_table_diff_params(self, col_to_json):
        self.spark_conf.storage_level = 'OFF_HEAP'
        test_table = 'test_table'
        json_cols = ('a', 'b')
        mode_param = 'test_mode'
        df = mock.MagicMock()

        col_to_json.return_value = df
        persist = df.persist
        format = df.write.format
        options = format.return_value.options
        mode = options.return_value.mode
        save = mode.return_value.save
        persist.return_value = df
        self.spark_pipeline.save_df_to_table(
            df, test_table, json_cols=json_cols, mode=mode_param
        )

        persist.assert_called_once_with(
            StorageLevelFactory.get_storage_level(self.spark_conf.storage_level))
        format.assert_called_once_with('jdbc')
        options.assert_called_once_with(
            url=self.spark_pipeline.db_url,
            driver=self.spark_pipeline.spark_conf.db_driver,
            dbtable=test_table,
            user=self.spark_pipeline.db_conf.user,
            password=self.spark_pipeline.db_conf.password,
            stringtype='unspecified',
            batchsize=100000,
            max_connections=1250,
            rewriteBatchedStatements=True,
            reWriteBatchedInserts=True,
            useServerPrepStmts=False
        )
        mode.assert_called_once_with(mode_param)

        called_args = []
        for args in col_to_json.call_args_list:
            self.assertEqual(args[0][0], df)
            self.assertTrue(args[0][1] in json_cols)
            called_args.append(args[0][1])

        self.assertSetEqual(set(called_args), set(json_cols))

        save.assert_called_once()

    def test_save_df_to_table(self):
        test_table_name = 'test'
        df = mock.MagicMock()
        persist = df.persist
        after_col_to_json = persist.return_value
        format = after_col_to_json.write.format
        options = format.return_value.options
        mode = options.return_value.mode
        save = mode.return_value.save

        self.spark_pipeline.save_df_to_table(
            df,
            test_table_name,
            json_cols=()
        )

        format.assert_called_once_with('jdbc')
        options.assert_called_once_with(
            url=self.spark_pipeline.db_url,
            driver=self.spark_pipeline.spark_conf.db_driver,
            dbtable=test_table_name,
            user=self.spark_pipeline.db_conf.user,
            password=self.spark_pipeline.db_conf.password,
            stringtype='unspecified',
            batchsize=100000,
            max_connections=1250,
            rewriteBatchedStatements=True,
            reWriteBatchedInserts=True,
            useServerPrepStmts=False
        )
        mode.assert_called_once_with('append')
        save.assert_called_once()

    @mock.patch('baskerville.spark.helpers.col_to_json')
    def test_save_df_to_table_json_cols(self, col_to_json):
        test_table_name = 'test'
        test_json_cols = ('a', 'b')
        df = mock.MagicMock()

        col_to_json.return_value = df
        persist = df.persist
        format = df.write.format
        options = format.return_value.options
        mode = options.return_value.mode
        save = mode.return_value.save
        persist.return_value = df

        self.spark_pipeline.save_df_to_table(
            df,
            test_table_name,
            json_cols=test_json_cols
        )

        format.assert_called_once_with('jdbc')
        options.assert_called_once_with(
            url=self.spark_pipeline.db_url,
            driver=self.spark_pipeline.spark_conf.db_driver,
            dbtable=test_table_name,
            user=self.spark_pipeline.db_conf.user,
            password=self.spark_pipeline.db_conf.password,
            stringtype='unspecified',
            batchsize=100000,
            max_connections=1250,
            rewriteBatchedStatements=True,
            reWriteBatchedInserts=True,
            useServerPrepStmts=False
        )
        mode.assert_called_once_with('append')
        save.assert_called_once()
        self.assertEqual(col_to_json.call_count, 2)
        actual_json_col = []
        for call in col_to_json.call_args_list:
            self.assertTrue(call[0][0] == df)
            self.assertTrue(call[0][1] in test_json_cols)
            actual_json_col.append(call[0][1])

        self.assertTupleEqual(tuple(actual_json_col), test_json_cols)

    @mock.patch('baskerville.models.base_spark.instantiate_from_str')
    @mock.patch('baskerville.models.base_spark.bytes')
    def test_filter_columns(self, mock_bytes, mock_instantiate_from_str):
        logs = [
            {
                'client_ip': '1',
                '@timestamp': '1',
                'client_request_host': 'testhost',
                'afeature': 0.1,
                'drop_if_null': 0.1,
            },
            {
                'client_ip': '1',
                '@timestamp': '1',
                'client_request_host': 'testhost',
                'afeature': 1.,
                'drop_if_null': None,
            }
        ]

        df = self.session.createDataFrame(logs)
        self.spark_pipeline.data_parser = self.get_data_parser_helper()
        self.spark_pipeline.engine_conf.data_config.timestamp_column = '@timestamp'
        # drop_if_null has 1 null value, the null should be dropped
        self.spark_pipeline.data_parser.drop_row_if_missing = [
            'client_ip', 'client_request_host', 'drop_if_null'
        ]
        self.spark_pipeline.feature_manager.get_active_columns = mock.MagicMock()
        self.spark_pipeline.feature_manager.get_active_columns.return_value = [
            'client_ip', 'client_request_host', 'afeature', 'drop_if_null'
        ]
        self.spark_pipeline.set_broadcasts = mock.MagicMock()
        model_index = mock.MagicMock()
        model_index.id = 1
        db_tools = self.mock_baskerville_tools.return_value
        db_tools.get_ml_model_from_db.return_value = model_index

        self.spark_pipeline.initialize()

        self.spark_pipeline.logs_df = df

        self.spark_pipeline.filter_columns()

        self.spark_pipeline.logs_df.show()
        df.select(*self.spark_pipeline.logs_df.columns).where(
            F.col('drop_if_null').isNotNull()
        ).show()

        self.assertEqual(self.spark_pipeline.logs_df.count(), 1)
        self.assertListEqual(sorted(self.spark_pipeline.logs_df.columns),
                             sorted(df.columns))
        self.assertDataFrameEqual(
            self.spark_pipeline.logs_df,
            df.select(*self.spark_pipeline.logs_df.columns).where(
                F.col('drop_if_null').isNotNull()
            )
        )

    def get_data_parser_helper(self):
        data_conf = DataParsingConfig({
            'parser': 'JSONLogSparkParser',
            'schema': f'{get_default_data_path()}/samples/log_schema.json',
            'timestamp_column': '@timestamp'
        })
        data_conf.validate()
        return data_conf.parser
