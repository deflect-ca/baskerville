# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import itertools

from baskerville.models.anomaly_model import AnomalyModel
from tests.unit.baskerville_tests.helpers.spark_testing_base import SQLTestCaseLatestSpark
import numpy as np
from pyspark.sql import functions as F


class TestAnomalyModel(SQLTestCaseLatestSpark):

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super()

    def test_basic(self):
        num_samples = 1000
        num_anomalies = 100
        np.random.seed(888)
        dim = 2
        labels = np.array([])
        feature_names = ['x1', 'x2']

        regulars = np.random.normal(loc=0, scale=2, size=(num_samples - num_anomalies, dim))
        labels = np.append(labels, np.zeros(num_samples - num_anomalies, dtype=int))
        anomalies = np.random.normal(loc=10, scale=2, size=(num_anomalies, dim))
        labels = np.append(labels, np.ones(num_anomalies, dtype=int))
        all_samples = np.append(regulars, anomalies, axis=0)
        all_samples = np.column_stack((all_samples, labels))

        df = self.session.createDataFrame(
            map(lambda x: (float(x[0]), float(x[1]), float(x[2])), all_samples),
            schema=feature_names + ['label']
        )

        df = df.withColumn('features',
                           F.create_map(*list(itertools.chain(*[(F.lit(f), F.col(f)) for f in feature_names]))))

        model = AnomalyModel(
            num_trees=10,
            features={'x1': {'categorical': False}, 'x2': {'categorical': False}}
        )
        model.train(df)
        df = model.predict(df)
        self.assertDataFrameEqual(df[['prediction']], df[['label']].withColumnRenamed('label', 'prediction'))
