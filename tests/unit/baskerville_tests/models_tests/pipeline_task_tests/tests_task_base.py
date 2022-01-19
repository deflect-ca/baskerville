# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from unittest import mock

from baskerville.models.config import BaskervilleConfig
from tests.unit.baskerville_tests.helpers.spark_testing_base import \
    SQLTestCaseLatestSpark
from tests.unit.baskerville_tests.helpers.utils import test_baskerville_conf


class TestTask(SQLTestCaseLatestSpark):
    def setUp(self):
        super().setUp()
        self.test_conf = test_baskerville_conf
        self.baskerville_config = BaskervilleConfig(self.test_conf).validate()

    def _helper_task_set_up(self, steps=()):
        from baskerville.models.pipeline_tasks.tasks_base import Task

        self.task = Task(
            self.baskerville_config, steps
        )

    def test_initialize(self):
        self._helper_task_set_up()

        step_one = mock.MagicMock()
        step_two = mock.MagicMock()
        self.task.steps = [step_one, step_two]

        with mock.patch.object(
                self.task.service_provider, 'initialize_db_tools_service'
        ) as mock_initialize_db_tools_service:
            with mock.patch.object(
                    self.task.service_provider, 'initialize_spark_service'
            ) as mock_initialize_spark_service:
                self.task.initialize()
                mock_initialize_db_tools_service.assert_called_once()
                mock_initialize_spark_service.assert_called_once()
                step_one.initialize.assert_called_once()
                step_two.initialize.assert_called_once()

    def test_run(self):
        step_one = mock.MagicMock()
        step_two = mock.MagicMock()
        mock_steps = [step_one, step_two]
        self._helper_task_set_up(mock_steps)
        self.task.run()

        for step in mock_steps:
            step.set_df.assert_called_once()
            step.set_df.return_value.run.assert_called_once()

        self.assertTrue(len(self.task.remaining_steps) == 0)

    def test_finish_up(self):
        self._helper_task_set_up()
        with mock.patch.object(
                self.task.service_provider, 'finish_up'
        ) as mock_finish_up:
            self.task.finish_up()
            mock_finish_up.assert_called_once()

    def test_reset(self):
        self._helper_task_set_up()
        with mock.patch.object(
                self.task.service_provider, 'reset'
        ) as mock_reset:
            self.task.reset()
            mock_reset.assert_called_once()


class TestCacheTask(SQLTestCaseLatestSpark):
    def setUp(self):
        super().setUp()
        self.test_conf = test_baskerville_conf
        self.baskerville_config = BaskervilleConfig(self.test_conf).validate()

    def _helper_task_set_up(self, steps=()):
        from baskerville.models.pipeline_tasks.tasks_base import CacheTask

        self.task = CacheTask(
            self.baskerville_config, steps
        )

    def test_initialize(self):
        self._helper_task_set_up()

        step_one = mock.MagicMock()
        step_two = mock.MagicMock()
        self.task.steps = [step_one, step_two]

        with mock.patch.object(
                self.task.service_provider, 'initialize_db_tools_service'
        ) as mock_initialize_db_tools_service:
            with mock.patch.object(
                    self.task.service_provider, 'initialize_spark_service'
            ) as mock_initialize_spark_service:
                with mock.patch.object(
                    self.task.service_provider,
                        'initialize_request_set_cache_service'
                ) as mock_initialize_request_set_cache_service:
                    self.task.initialize()
                    mock_initialize_db_tools_service.assert_called_once()
                    mock_initialize_spark_service.assert_called_once()
                    mock_initialize_request_set_cache_service.\
                        assert_called_once()
                    step_one.initialize.assert_called_once()
                    step_two.initialize.assert_called_once()


class TestMLTask(SQLTestCaseLatestSpark):
    def setUp(self):
        super().setUp()
        self.test_conf = test_baskerville_conf
        self.baskerville_config = BaskervilleConfig(self.test_conf).validate()

    def _helper_task_set_up(self, steps=()):
        from baskerville.models.pipeline_tasks.tasks_base import MLTask

        self.task = MLTask(
            self.baskerville_config, steps
        )

    def test_initialize(self):
        self._helper_task_set_up()

        step_one = mock.MagicMock()
        step_two = mock.MagicMock()
        self.task.steps = [step_one, step_two]
        self.task.service_provider = mock.MagicMock()
        self.task.initialize()
        self.task.service_provider.initialize_db_tools_service\
            .assert_called_once()
        self.task.service_provider\
            .initialize_spark_service.assert_called_once()
        self.task.service_provider.initialize_request_set_cache_service. \
            assert_called_once()
        self.task.service_provider.initalize_ml_services.assert_called_once()
        step_one.initialize.assert_called_once()
        step_two.initialize.assert_called_once()
