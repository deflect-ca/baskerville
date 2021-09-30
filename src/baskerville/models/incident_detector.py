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
                 decision_delay_in_minutes=5,
                 logger=None):
        super().__init__()
        self.db_config = db_config
        self.logger = logger
        self.average_window_in_hours = average_window_in_hours
        self.candidates = None
        self.incidents = None
        self.decision_delay_in_minutes = decision_delay_in_minutes
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
        batch['ts'] = datetime.datetime.utcnow()
        anomalies = batch[batch['score'] > batch['avg'] + 2 * batch['stddev']]
        regulars = batch[batch['score'] <= batch['avg'] + 2 * batch['stddev']]

        self.stop_incidents(regulars)
        self.start_incidents(anomalies)

    def stop_incidents(self, regulars):
        if self.incidents is None:
            return
        stopped_incidents = pd.merge(self.incidents, regulars[['target', 'ts']], how='left', on='target')
        stopped_incidents = stopped_incidents[
            (self.incidents['ts_y'] - self.incidents['ts_x']) / pd.Timedelta(minutes=1) >
            self.decision_delay_in_minutes]

        # update stop timestamp in the database
        try:
            session, engine = set_up_db(self.db_config.__dict__)
            for index, row in stopped_incidents.iterrows():
                stopped_incidents.at[index, 'id'] = 50

            session.close()
            engine.dispose()

        except Exception as e:
            print(str(e))
            if self.logger:
                self.logger.error(str(e))
            return

        # remove stopped incidents
        self.incidents = pd.merge(self.incidents,
                                  stopped_incidents[['target']], how='outer', on='target', indicator=True)
        self.incidents = self.incidents[self.incidents['_merge'] == 'left_only']
        self.incidents = self.incidents.drop('_merge', 1)

    def start_incidents(self, anomalies):
        if self.candidates is None:
            self.candidates = anomalies
            return

        self.candidates = pd.merge(self.candidates, anomalies, how='outer', on='target', indicator=True)
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
        new_incidents = self.candidates[(self.candidates['ts_y'] - self.candidates['ts_x']) / pd.Timedelta(minutes=1) >
                                        self.decision_delay_in_minutes][['target', 'score', 'ts']]
        self.candidates = self.candidates.drop(
            ['_merge', 'score_x', 'score_y', 'avg_x', 'avg_y', 'stddev_x', 'stddev_y', 'ts_x', 'ts_y'], 1)

        # remove detected incidents from candidates
        self.candidates = pd.merge(self.candidates, new_incidents[['target']], how='outer', on='target', indicator=True)
        self.candidates = self.candidates[self.candidates['_merge'] == 'left_only']
        self.candidates = self.candidates.drop('_merge', 1)

        if new_incidents.empty:
            return

        # save the new incidents
        new_incidents['id'] = 0
        try:
            session, engine = set_up_db(self.db_config.__dict__)
            for index, row in new_incidents.iterrows():
                # attack = Attack()
                # attack.start = row['ts'].strftime('%Y-%m-%d %H:%M:%SZ')
                # attack.stop = row['ts'].strftime('%Y-%m-%d %H:%M:%SZ')
                # attack.target = row['target']
                # session.add(attack)
                # session.commit()

                new_incidents.at[index, 'id'] = 50

            session.close()
            engine.dispose()

        except Exception as e:
            print(str(e))
            if self.logger:
                self.logger.error(str(e))
            return

        if self.incidents is None:
            self.incidents = new_incidents
            return

        self.incidents = pd.concat(self.incidents, new_incidents)
