# Copyright (c) 2021, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
import threading

from baskerville.db import set_up_db
from baskerville.db.models import Attack
from baskerville.util.db_reader import DBReader
import datetime
import pandas as pd
import numpy as np


class IncidentDetector:

    def __init__(self,
                 db_config,
                 time_bucket_in_seconds=120,
                 time_horizon_in_seconds=600,
                 check_interval_in_seconds=60,
                 stat_refresh_period_in_minutes=60,
                 stat_window_in_hours=6,
                 min_traffic=3,
                 sigma_score=1,
                 sigma_traffic=1,
                 logger=None):
        super().__init__()
        self.kill = threading.Event()
        self.check_interval_in_seconds = check_interval_in_seconds
        self.time_bucket_in_seconds = time_bucket_in_seconds
        self.time_horizon_in_seconds = time_horizon_in_seconds
        self.sigma_score = sigma_score
        self.sigma_traffic = sigma_traffic
        self.min_traffic = min_traffic
        self.db_config = db_config
        self.logger = logger
        self.thread = None
        self.last_timestamp = None
        self.average_window_in_hours = stat_window_in_hours
        self.stats_reader = DBReader(db_config, refresh_period_in_minutes=stat_refresh_period_in_minutes, logger=logger)
        self.reader_traffic = DBReader(db_config, refresh_period_in_minutes=stat_refresh_period_in_minutes,
                                       logger=logger)
        self.candidates = None
        self.incidents = None

    def _run(self):
        while True:
            self._detect()
            is_killed = self.kill.wait(self.check_interval_in_seconds)
            if is_killed:
                break

    def start(self):
        if self.thread is not None:
            return

        self.thread = threading.Thread(target=self._run)
        self.thread.start()

    def stop(self):
        if self.thread is None:
            return
        self.kill.set()

    def _read_sample(self):
        try:
            session, engine = set_up_db(self.db_config.__dict__)
            stop = (datetime.datetime.utcnow() - 2 * datetime.timedelta(
                seconds=self.time_horizon_in_seconds)).strftime("%Y-%m-%d %H:%M:%S %z")
            query = f'SELECT floor(extract(epoch from stop)/{self.time_bucket_in_seconds})*' \
                    f'{self.time_bucket_in_seconds} AS "time", target, ' \
                    f'count(distinct ip) as traffic, (sum(prediction*1.0) / count(ip)) as challenged_portion ' \
                    f'FROM request_sets WHERE stop > \'{stop}\' ' \
                    f'and floor(extract(epoch from stop)/{self.time_bucket_in_seconds})*' \
                    f'{self.time_bucket_in_seconds} in ' \
                    f'(' \
                    f'  select max(floor(extract(epoch from stop)/{self.time_bucket_in_seconds})*' \
                    f'{self.time_bucket_in_seconds}) from request_sets ' \
                    f'  WHERE stop > \'{stop}\' ' \
                    f' ) ' \
                    ' group by 1, 2 order by 1'

            data = pd.read_sql(query, engine)
            session.close()
            engine.dispose()

            if data.empty:
                return None

            if self.last_timestamp is not None:
                if data['time'][0] == self.last_timestamp:
                    return None

            self.last_timestamp = data['time'][0]
            return data

        except Exception as e:
            print(str(e))
            if self.logger:
                self.logger.error(str(e))
            return None

    def _start_incidents(self, anomalies):
        if anomalies is None:
            return

        if self.incidents is None:
            new_incidents = anomalies
        else:
            new_incidents = pd.merge(anomalies, self.incidents[['target']], how='outer', indicator=True)
            new_incidents = new_incidents[new_incidents['_merge'] == 'left_only']
            new_incidents = new_incidents.drop('_merge', 1)

        if new_incidents.empty:
            return

        # save the new incidents
        new_incidents['id'] = 0
        new_incidents['start'] = pd.to_datetime(new_incidents['time'], unit='s', utc=True)
        new_incidents = new_incidents.drop('time', 1)

        try:
            session, engine = set_up_db(self.db_config.__dict__)
            for index, row in new_incidents.iterrows():
                attack = Attack()
                attack.start = row['start'].strftime('%Y-%m-%d %H:%M:%SZ')
                attack.target = row['target']
                session.add(attack)
                session.commit()
                new_incidents.at[index, 'id'] = attack.id

                target = row['target']
                self.logger.info(f'New incident, target={target}, id={attack.id}')

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

        self.incidents = pd.concat([self.incidents, new_incidents])

    def _stop_incidents(self, regulars):
        if self.incidents is None:
            return

        stopped_incidents = pd.merge(self.incidents, regulars[['target', 'time']], how='inner', on='target')
        if len(stopped_incidents) == 0:
            return

        stopped_incidents['stop'] = pd.to_datetime(stopped_incidents['time'], unit='s', utc=True)
        stopped_incidents = stopped_incidents.drop('time', 1)

        # update stop timestamp in the database
        try:
            session, engine = set_up_db(self.db_config.__dict__)
            for index, row in stopped_incidents.iterrows():
                session.query(Attack).filter(Attack.id == row['id']).update({'stop': row['stop'].strftime('%Y-%m-%d %H:%M:%SZ')})
                target = row['target']
                attack_id = row['id']
                self.logger.info(f'Incident finished, target={target}, id = {attack_id}')

            session.commit()
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

    def _detect(self):
        stop = (datetime.datetime.utcnow() - datetime.timedelta(
            hours=self.average_window_in_hours)).strftime("%Y-%m-%d %H:%M:%S %z")

        self.stats_reader.set_query(
            f'select target, avg(traffic) as avg_traffic, stddev(traffic) as stddev_traffic, '
            f'avg(challenged_portion) as avg_challenged_portion, '
            f'stddev(challenged_portion) as stddev_challenged_portion from'
            f'('
            f'SELECT floor(extract(epoch from stop)/120)*120 AS "time", target, count(ip) as traffic, '
            f'(sum(prediction*1.0) / count(ip)) as challenged_portion '
            f'FROM request_sets WHERE stop > \'{stop}\' '
            f'group by 1, 2'
            f') a '
            f'group by target'
        )
        stats = self.stats_reader.get()

        if stats is None:
            return

        stats = stats[(~stats['avg_traffic'].isnull()) & (~stats['stddev_traffic'].isnull())
                      & (~stats['avg_challenged_portion'].isnull()) & (~stats['stddev_challenged_portion'].isnull())
                      & (stats['avg_traffic'] > self.min_traffic)
                      & (stats['avg_challenged_portion'] > 0)
                      & (stats['avg_challenged_portion'] < 0.6)]

        sample = self._read_sample()
        if sample is None:
            return

        batch = pd.merge(sample, stats, how='left', on='target')

        condition = (batch['challenged_portion'] > (batch['avg_challenged_portion'] +
                                                    self.sigma_score * batch['stddev_challenged_portion'])) & \
                    (batch['traffic'] > (batch['avg_traffic'] + self.sigma_traffic * batch['stddev_traffic']))

        anomalies = batch[condition]
        regulars = batch[~condition]

        self._stop_incidents(regulars)
        self._start_incidents(anomalies)
