# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from enum import Enum


class BaseStrEnum(Enum):

    def __str__(self):
        return self.value

    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        elif isinstance(other, self.__class__):
            return self.value == other.value
        return False

    def __ne__(self, other):
        if isinstance(other, str):
            return self.value != other
        elif isinstance(other, self.__class__):
            return self.value != other.value
        return True

    def __hash__(self):
        return hash(self.value)


class LogType(Enum):
    apache = 0
    nginx = 0
    raw = 0
    csv = 0
    json = 0


class RunType(BaseStrEnum):
    es = 'es'
    rawlog = 'rawlog'
    kafka = 'kafka'
    training = 'training'
    client = 'client'
    irawlog = 'irawlog'


class Step(BaseStrEnum):
    initialize = 'Start sessions, initialize cache/features/model/dfs.'
    create_runtime = 'Create a Runtime in the Baskerville database.'
    get_data = 'Get dataframe of data.'
    get_window = 'Select data from current time bucket window.'
    preprocessing = 'Fill missing values, add calculation cols, and filter.'
    group_by = 'Group logs by IP/host.'
    feature_calculation = 'Add calculation cols, extract features, and update.'
    label_or_predict = 'Apply label from MISP or predict label.'
    trigger_challenge = 'Trigger host wise challenge'
    save = 'Save to database.'
    finish_up = 'Disconnect from db, unpersist dataframes, and empty cache.'
    train = 'Train'
    test = 'Test'
    evaluate = 'Evaluate results'
    reset = 'Reset: remove stored data'


class NotifyMetricEnum(BaseStrEnum):
    total_requests_count = 'total_requests_count'
    total_request_sets_count = 'total_request_sets_count'
    total_alerts_count = 'total_alerts_count'
    current_requests_count = 'current_requests_count'
    requests_count_so_far = 'requests_count_so_far'
    current_request_sets_count = 'current_request_sets_count'
    request_sets_count_so_far = 'request_sets_count_so_far'
    subsets_count_so_far = 'subsets_count_so_far'
    current_alerts_count = 'current_alerts_count'
    alerts_count_so_far = 'alerts_count_so_far'


class FeatureComputeType(BaseStrEnum):
    mean = 'update_mean'
    variance = 'update_variance'
    total = 'update_total'
    rate = 'update_rate'
    maximum = 'update_maximum'
    minimum = 'update_minimum'
    ratio = 'update_ratio'
    replace = 'update_replace'
    other = 'update_other'


class LabelEnum(Enum):
    """
    Following the Isolation Forest's initial paper:
    benign 0 <= prediction <= 1 malicious
    """
    malicious = 1
    benign = 0
    unknown = 42

    def __eq__(self, other):
        if isinstance(other, LabelEnum):
            return self.value == other.value
        return self.value == other


class MetricTypeEnum(BaseStrEnum):
    progress = "Progress"
    performance = "Performance"
    exception = "Exception"


class MetricClassEnum(BaseStrEnum):
    counter = "counter"
    gauge = "gauge"
    summary = "summary"
    histogram = "histogram"
    info = "info"
    enum = "enum"


class PartitionByEnum(BaseStrEnum):
    w = 'week'
    m = 'month'


class ModelEnum(BaseStrEnum):
    AnomalyModelSklearn = "baskerville.models.anomaly_model_sklearn.AnomalyModelSklearn"
    AnomalyModel = "baskerville.models.anomaly_model.AnomalyModel"
