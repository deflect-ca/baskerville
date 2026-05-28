# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.models.pipelines import RawLogPipeline, KafkaPipeline
from baskerville.util.enums import RunType

PIPELINE_TO_RUN_TYPE = {
    RawLogPipeline.__name__: RunType.rawlog,
    KafkaPipeline.__name__: RunType.kafka,
}

PIPELINE_TO_CONFIG = {
    RawLogPipeline.__name__: ['database', 'engine', 'spark'],
    KafkaPipeline.__name__: ['database', 'kafka', 'engine', 'spark'],
}
