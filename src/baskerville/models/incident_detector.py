# Copyright (c) 2021, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from baskerville.db import set_up_db
from baskerville.db.models import Attack
from baskerville.util.db_reader import DBReader
import datetime
from pyspark.sql import functions as F
import pandas as pd
import numpy as np


class IncidentDetector:

    def __init__(self,
                 db_config,
                 stat_refresh_period_in_minutes=60,
                 average_window_in_hours=1,
                 min_attack_duration_in_minutes=5,
                 logger=None):
        super().__init__()
        self.db_config = db_config
        self.logger = logger
        self.average_window_in_hours = average_window_in_hours
        self.candidates = None
        self.min_attack_duration_in_minutes = min_attack_duration_in_minutes
        self.db_reader = DBReader(
            db_config, refresh_period_in_minutes=stat_refresh_period_in_minutes, logger=logger)

    def run(self, df):
        stop = (datetime.datetime.utcnow() - datetime.timedelta(
            hours=self.average_window_in_hours)).strftime("%Y-%m-%d %H:%M:%S %z")
        self.db_reader.set_query(
            f'select target, avg(score), stddev(score) '
            f'from request_sets rs where stop > \'{stop}\' '
            f'group by 1 '
        )
        stats = self.db_reader.get()
        if stats is None:
            return

        target_stats = stats.set_index('target')
        target_score = df.select(['target', 'score']).groupby('target').agg(F.avg('score').alias('score')).toPandas()
        batch = pd.merge(target_score, target_stats, how='left', on='target')
        batch = batch[batch['score'] > batch['avg'] + 2 * batch['stddev']]
        batch['ts'] = datetime.datetime.utcnow()

        if self.candidates is None:
            self.candidates = batch
            return

        self.candidates = pd.merge(self.candidates, batch, how='outer', on='target', indicator=True)
        self.candidates = self.candidates[self.candidates['_merge'] != 'left_only']

        self.candidates['ts'] = np.where(self.candidates['_merge'] == 'right_only',
                                         self.candidates['ts_y'], self.candidates['ts_x'])
        self.candidates['score'] = np.where(self.candidates['_merge'] == 'right_only',
                                            self.candidates['score_y'], self.candidates['score_x'])
        self.candidates['avg'] = np.where(self.candidates['_merge'] == 'right_only',
                                          self.candidates['avg_y'], self.candidates['avg_x'])
        self.candidates['stddev'] = np.where(self.candidates['_merge'] == 'right_only',
                                             self.candidates['stddev_y'], self.candidates['stddev_x'])

        # get long enough candidates as incidents
        incidents = self.candidates[(self.candidates['ts_y'] - self.candidates['ts_x']) / pd.Timedelta(minutes=1) >
                                    self.min_attack_duration_in_minutes][['target', 'score', 'ts']]
        self.candidates = self.candidates.drop(
            ['_merge', 'score_x', 'score_y', 'avg_x', 'avg_y', 'stddev_x', 'stddev_y', 'ts_x', 'ts_y'], 1)

        # remove detected incidents from candidates
        self.candidates = pd.merge(self.candidates, incidents[['target']], how='outer', on='target', indicator=True)
        self.candidates = self.candidates[self.candidates['_merge'] == 'left_only']
        self.candidates = self.candidates.drop('_merge', 1)

        # save the incidents
        if not incidents.empty:
            incidents['id'] = 0
            try:
                session, engine = set_up_db(self.db_config.__dict__)
                for index, row in incidents.iterrows():
                    # attack = Attack()
                    # attack.start = row['ts'].strftime('%Y-%m-%d %H:%M:%SZ')
                    # attack.stop = row['ts'].strftime('%Y-%m-%d %H:%M:%SZ')
                    # attack.target = row['target']
                    # session.add(attack)
                    # session.commit()

                    incidents.at[index, 'id'] = 50

                session.close()
                engine.dispose()

            except Exception as e:
                print(str(e))
                if self.logger:
                    self.logger.error(str(e))
                return None



            import pdb
            pdb.set_trace()


