# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from baskerville.models.config import BaskervilleConfig
from baskerville.models.pipeline_tasks.tasks_base import Task
import os


class IncidentLoader(Task):
    """
    Reads data from s3 cloud storage.
    """

    def __init__(
            self,
            config: BaskervilleConfig,
            steps: list = (),
            incident_ids=[]
    ):
        super().__init__(config, steps)
        self.incident_ids = incident_ids

    def load(self):
        """
        Loads all the data from s3
        :return:
        :rtype: pyspark.sql.Dataframe
        """
        dfs = []
        for attack_id in self.incident_ids:
            df = self.spark.read.parquet(os.path.join(self.config.engine.incidents_path, f'{attack_id}'))
            df = df.select(['ip', 'target', 'stop', 'features', 'label'])
            dfs.append(df)
        df = dfs[0]
        for i in range(1, len(dfs)):
            df = df.union(dfs[i])

        return df

    def run(self):
        self.df = self.load()
        self.df = super().run()
        return self.df
