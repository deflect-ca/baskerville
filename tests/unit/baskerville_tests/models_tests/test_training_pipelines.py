import sys
from unittest import mock

from baskerville.models.config import DatabaseConfig, EngineConfig, SparkConfig
from tests.unit.baskerville_tests.helpers.spark_testing_base import \
    SQLTestCaseLatestSpark


class TestTrainingSparkMLPipeline(SQLTestCaseLatestSpark):
    def setUp(self):
        if 'baskerville.util.helpers' in sys.modules:
            del sys.modules['baskerville.util.helpers']
        if 'baskerville.models.pipeline_training' in sys.modules:
            del sys.modules['baskerville.models.pipeline_training']
        if 'baskerville.models.model_manager' in sys.modules:
            del sys.modules['baskerville.models.model_manager']
        if 'tests.unit.baskerville_tests.utils_tests.test_helpers' in sys.modules:
            del sys.modules['tests.unit.baskerville_tests.utils_tests.test_helpers']

        self.db_tools_patcher = mock.patch(
            'baskerville.util.baskerville_tools.BaskervilleDBTools'
        )
        self.instantiate_from_str_patcher = mock.patch(
            'baskerville.util.helpers.instantiate_from_str'
        )
        self.mock_baskerville_tools = self.db_tools_patcher.start()
        self.mock_instantiate_from_str = self.instantiate_from_str_patcher.start()
        self.training_conf = {
            'model_name': 'test_model',
            'scaler_name': 'test_scaler',
            'path': 'test_scaler',
            'classifier': 'pyspark_iforest.ml.iforest.IForest',
            'scaler': 'pyspark.ml.feature.StandardScaler',
            'data_parameters': {
                'training_days': 30,
                'from_date': '2020-02-25',
                'to_date': '2020-03-15',
            },
            'classifier_parameters': {
                'maxSamples': 10000,
                'contamination': 0.1,
                'numTrees': 1000
            }
        }
        self.db_conf = DatabaseConfig({}).validate()
        self.engine_conf = EngineConfig({
            'log_level': 'INFO',
            'training': self.training_conf,
            'extra_features': ['a', 'b', 'c']
        }).validate()

        # from baskerville.util.helpers import get_default_data_path
        # iforest_jar = f'{get_default_data_path()}/jars/spark-iforest-2.4.0.jar'
        self.spark_conf = SparkConfig({
            'db_driver': 'test',
            # 'jars': iforest_jar,
            # 'driver_extra_class_path': iforest_jar
        }).validate()

        from baskerville.models.pipeline_training import \
            TrainingSparkMLPipeline

        self.pipeline = TrainingSparkMLPipeline(
            self.db_conf,
            self.engine_conf,
            self.spark_conf
        )
        super().setUp()

    @mock.patch('baskerville.util.helpers.instantiate_from_str')
    def test_initialize(self, mock_instantiate_from_str):
        # todo: some reference to instantiate_from_str prevents the correct
        #  mocking - thus this test fails when ran with all other tests
        pass
        # self.pipeline.initialize()
        # self.assertTrue(self.pipeline.spark is not None)
        # self.assertTrue(self.pipeline.feature_manager is not None)
        # self.assertTrue(self.pipeline.scaler is not None)
        # self.assertTrue(self.pipeline.classifier is not None)
        # self.pipeline.classifier.setParams.assert_called()
        # self.pipeline.scaler.setParams.assert_called()
        # self.pipeline.classifier.setSeed.assert_called_once_with(42)

    # def test_get_data(self):
    #     raise NotImplementedError()
    #
    # def test_train(self):
    #     raise NotImplementedError()
    #
    # def test_test(self):
    #     raise NotImplementedError()
    #
    # def test_evaluate(self):
    #     raise NotImplementedError()
    #
    # def test_save(self):
    #     raise NotImplementedError()
    #
    # def test_get_bounds(self):
    #     raise NotImplementedError()
    #
    # def test_load(self):
    #     raise NotImplementedError()
    #
    # def test_finish_up(self):
    #     raise NotImplementedError()

