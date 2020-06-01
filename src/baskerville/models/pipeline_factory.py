from baskerville.models.pipeline_training import TrainingPipeline
from baskerville.models.pipelines import RawLogPipeline, ElasticsearchPipeline, KafkaPipeline
from baskerville.util.enums import RunType


class PipelineFactory(object):
    def get_pipeline(self, run_type,  config):
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
        raise RuntimeError(
            'Cannot set up a pipeline with the current configuration.'
        )
