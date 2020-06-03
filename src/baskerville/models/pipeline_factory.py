# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.pipeline_training import TrainingPipeline
from baskerville.models.pipelines import RawLogPipeline, ElasticsearchPipeline, KafkaPipeline
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
        elif run_type == RunType.training:
            return TrainingPipeline(
                config.database,
                config.engine,
                config.spark
            )
        elif run_type == RunType.client:
            from baskerville.client_pipeline import set_up_client_processing_pipeline
            return set_up_client_processing_pipeline(config)
        elif run_type == RunType.irawlog:
            from baskerville.rawlog_pipeline import set_up_irawlog_pipeline
            return set_up_irawlog_pipeline(config)

        raise RuntimeError(
            'Cannot set up a pipeline with the current configuration.'
        )
