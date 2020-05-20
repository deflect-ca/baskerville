from baskerville.models.pipelines import ElasticsearchPipeline, \
    RawLogPipeline, KafkaPipeline
from baskerville.util.enums import RunType

PIPELINE_TO_RUN_TYPE = {
    ElasticsearchPipeline.__name__: RunType.es,
    RawLogPipeline.__name__: RunType.rawlog,
    KafkaPipeline.__name__: RunType.kafka,
}

PIPELINE_TO_CONFIG = {
    ElasticsearchPipeline.__name__: ['database', 'elastic', 'engine', 'spark'],
    RawLogPipeline.__name__: ['database', 'engine', 'spark'],
    KafkaPipeline.__name__: ['database', 'kafka', 'engine', 'spark'],
}
