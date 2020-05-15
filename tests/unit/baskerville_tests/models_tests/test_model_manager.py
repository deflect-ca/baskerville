import _pickle as cPickle
from unittest import mock

import pyspark
from baskerville.models.anomaly_detector import AnomalyDetector, \
    ScikitAnomalyDetectorManager, SparkAnomalyDetectorManager
from baskerville.models.config import DatabaseConfig, EngineConfig
from baskerville.models.model_manager import ModelManager
from baskerville.util.enums import AlgorithmEnum

from tests.unit.baskerville_tests.helpers.spark_testing_base import \
    SQLTestCaseLatestSpark


class TestModelManager(SQLTestCaseLatestSpark):
    def setUp(self):
        super(TestModelManager, self).setUp()
        self.db_tools_patcher = mock.patch(
            'baskerville.util.baskerville_tools.BaskervilleDBTools'
        )
        self.spark_patcher = mock.patch(
            'baskerville.models.base_spark.SparkPipelineBase.'
            'instantiate_spark_session'
        )
        self.mock_db_tools = self.db_tools_patcher.start()
        self.mock_spark = self.spark_patcher.start()
        self.mock_db_conf = DatabaseConfig({}).validate()
        self.mock_engine_conf = EngineConfig({
            'log_level': 'INFO'
        }).validate()
        self.model_manager = ModelManager(
            self.mock_db_conf, self.mock_engine_conf, self.session
        )

    def test_initialize(self):
        test_session = 'test session'
        test_db_tools = 'test db_tools'

        with mock.patch.object(
                ModelManager, 'load'
        ) as mock_load:

            self.model_manager = ModelManager(
                self.mock_db_conf, self.mock_engine_conf
            )
            self.model_manager.initialize(test_session, test_db_tools)
            self.assertEqual(self.model_manager.spark_session, test_session)
            self.assertEqual(self.model_manager.db_tools, test_db_tools)
            mock_load.assert_called_once()

    def test_get_active_model_model_id_scikit(self):
        with mock.patch.object(
                ScikitAnomalyDetectorManager, 'load'
        ) as mock_manager:
            # mock_engine_conf = mock.MagicMock()
            mock_db_tools = mock.MagicMock()
            self.mock_engine_conf.model_id = 1
            mock_model_value = mock.MagicMock()
            mock_model_value.algorithm = AlgorithmEnum.isolation_forest_sklearn
            mock_db_tools.get_ml_model_from_db.return_value = mock_model_value
            self.model_manager = ModelManager(
                self.mock_db_conf, self.mock_engine_conf, db_tools=mock_db_tools
            )

            active_model = self.model_manager.load()
            self.assertEqual(active_model, mock_model_value)
            mock_db_tools.get_ml_model_from_db.assert_called_once_with(
                self.mock_engine_conf.model_id
            )
            mock_manager.assert_called_once()

    def test_get_active_model_model_id_sparkml(self):
        with mock.patch.object(
                SparkAnomalyDetectorManager, 'load'
        ) as mock_manager:
            # mock_engine_conf = mock.MagicMock()
            mock_db_tools = mock.MagicMock()
            self.mock_engine_conf.model_id = 1
            mock_model_value = mock.MagicMock()
            mock_model_value.algorithm = AlgorithmEnum.isolation_forest_pyspark
            mock_db_tools.get_ml_model_from_db.return_value = mock_model_value
            self.model_manager = ModelManager(
                self.mock_db_conf, self.mock_engine_conf, db_tools=mock_db_tools
            )

            active_model = self.model_manager.load()
            self.assertEqual(active_model, mock_model_value)
            mock_db_tools.get_ml_model_from_db.assert_called_once_with(
                self.mock_engine_conf.model_id
            )
            mock_manager.assert_called_once()

    def test_get_active_model_model_path_scikit(self):
        with mock.patch.object(
                ScikitAnomalyDetectorManager, 'load'
        ) as mock_manager:
            self.mock_engine_conf.model_id = None
            self.mock_engine_conf.model_path = 'a path'
            mock_model_value = mock.MagicMock()
            mock_model_value.algorithm = AlgorithmEnum.isolation_forest_sklearn
            mock_model_value.classifier = cPickle.dumps({})
            mock_model_value.scaler = cPickle.dumps({})
            mock_model_value.host_encoder = cPickle.dumps({})
            self.mock_db_tools.get_ml_model_from_file.return_value = mock_model_value
            self.model_manager = ModelManager(
                self.mock_db_conf, self.mock_engine_conf, db_tools=self.mock_db_tools
            )
            active_model = self.model_manager.load()
            self.assertEqual(active_model, mock_model_value)
            self.mock_db_tools.get_ml_model_from_file.assert_called_once_with(
                self.mock_engine_conf.model_path
            )

    def test_get_active_model_model_path_spark_ml(self):
        with mock.patch.object(
                SparkAnomalyDetectorManager, 'load'
        ) as mock_manager:
            self.mock_engine_conf.model_id = None
            self.mock_engine_conf.model_path = 'a path'
            mock_model_value = mock.MagicMock()
            mock_model_value.algorithm = AlgorithmEnum.isolation_forest_pyspark
            mock_model_value.classifier = cPickle.dumps({})
            mock_model_value.scaler = cPickle.dumps({})
            mock_model_value.host_encoder = cPickle.dumps({})
            self.mock_db_tools.get_ml_model_from_file.return_value = mock_model_value
            self.model_manager = ModelManager(
                self.mock_db_conf, self.mock_engine_conf, db_tools=self.mock_db_tools
            )
            active_model = self.model_manager.load()
            self.assertEqual(active_model, mock_model_value)
            self.mock_db_tools.get_ml_model_from_file.assert_called_once_with(
                self.mock_engine_conf.model_path
            )