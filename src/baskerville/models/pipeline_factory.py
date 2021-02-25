# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.pipeline_training import TrainingPipeline
from baskerville.models.pipelines import RawLogPipeline, \
    ElasticsearchPipeline, KafkaPipeline
from baskerville.util.enums import RunType


class PipelineFactory(object):
    def get_pipeline(self, run_type, config):
        if run_type == RunType.es:
            return ElasticsearchPipeline(
                config.database,
                config.elastic,
                config.engine,
                config.spark
            )
        elif run_type == RunType.rawlog:
            return RawLogPipeline(
                config.database,
                config.engine,
                config.spark
            )
        elif run_type == RunType.kafka:
            return KafkaPipeline(
                config.database,
                config.engine,
                config.kafka,
                config.spark
            )
        elif run_type == RunType.training_old:
            return TrainingPipeline(
                config.database,
                config.engine,
                config.spark
            )
        elif run_type == RunType.preprocessing:
            from baskerville.models.pipeline_tasks.client_pipeline \
                import set_up_preprocessing_pipeline
            return set_up_preprocessing_pipeline(config)
        elif run_type == RunType.postprocessing:
            from baskerville.models.pipeline_tasks.client_pipeline \
                import set_up_postprocessing_pipeline
            return set_up_postprocessing_pipeline(config)
        elif run_type == RunType.irawlog:
            from baskerville.models.pipeline_tasks.rawlog_pipeline \
                import set_up_isac_rawlog_pipeline
            return set_up_isac_rawlog_pipeline(config)
        elif run_type == RunType.ikafka:
            from baskerville.models.pipeline_tasks.tasks_base \
                import set_up_isac_kafka_pipeline
            return set_up_isac_kafka_pipeline(config)
        elif run_type == RunType.predicting:
            from baskerville.models.pipeline_tasks.prediction_pipeline \
                import set_up_prediction_pipeline
            return set_up_prediction_pipeline(config)
        elif run_type == RunType.training:
            from baskerville.models.pipeline_tasks.training_pipeline \
                import set_up_training_pipeline
            return set_up_training_pipeline(config)
        # elif run_type == RunType.dashboard_preprocessing:
        #     from baskerville.models.pipeline_tasks.dashboard_pipeline import \
        #         set_up_dashboard_preprocessing_pipeline
        #     return set_up_dashboard_preprocessing_pipeline(config)
        # elif run_type == RunType.dashboard:
        #     from baskerville.models.pipeline_tasks.dashboard_pipeline import \
        #         set_up_dashboard_pipeline
        #     return set_up_dashboard_pipeline(config)
        elif run_type == RunType.client_rawlog:
            from baskerville.models.pipeline_tasks.client_pipeline import \
                set_up_client_rawlog_pipeline
            return set_up_client_rawlog_pipeline(config)
        elif run_type == RunType.feedback:
            from baskerville.models.pipeline_tasks.feedback_pipeline import \
                set_up_feedback_pipeline
            return set_up_feedback_pipeline(config)

        raise RuntimeError(
            'Cannot set up a pipeline with the current configuration.'
        )
