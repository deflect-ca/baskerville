# Copyright (c) 2021, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
import threading
import time

from baskerville.db import set_up_db
from baskerville.db.models import Attack
from baskerville.util.db_reader import DBReader
import datetime
import pandas as pd


class IncidentDetector:

    def __init__(self,
                 db_config,
                 time_bucket_in_seconds=120,
                 time_horizon_in_seconds=600,
                 check_interval_in_seconds=60,
                 stat_refresh_period_in_minutes=30,
                 stat_window_in_hours=1,
                 min_traffic=3,
                 min_traffic_incident=50,
                 min_challenged_portion_incident=0.5,
                 sigma_score=2.5,
                 sigma_traffic=2.5,
                 dashboard_url_prefix=None,
                 dashboard_minutes_before=60,
                 dashboard_minutes_after=120,
                 logger=None,
                 mail_sender=None,
                 emails=None):
        super().__init__()
        self.kill = threading.Event()
        self.check_interval_in_seconds = check_interval_in_seconds
        self.time_bucket_in_seconds = time_bucket_in_seconds
        self.time_horizon_in_seconds = time_horizon_in_seconds
        self.sigma_score = sigma_score
        self.sigma_traffic = sigma_traffic
        self.min_traffic = min_traffic
        self.min_traffic_incident = min_traffic_incident
        self.min_challenged_portion_incident = min_challenged_portion_incident
        self.db_config = db_config
        self.logger = logger
        self.mail_sender = mail_sender
        self.emails = emails
        self.thread = None
        self.last_timestamp = None
        self.average_window_in_hours = stat_window_in_hours
        self.stats_reader = DBReader(db_config, refresh_period_in_minutes=stat_refresh_period_in_minutes, logger=logger)
        self.reader_traffic = DBReader(db_config, refresh_period_in_minutes=stat_refresh_period_in_minutes,
                                       logger=logger)
        self.candidates = None
        self.incidents = None
        self.dashboard_url_prefix = dashboard_url_prefix
        self.dashboard_minutes_before = dashboard_minutes_before
        self.dashboard_minutes_after = dashboard_minutes_after

    def _run(self):
        if self.logger:
            self.logger.info('Starting incident detector...')
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
        self.thread.join()

    def _get_dashboard_url(self, start, target):
        ts_from = start - datetime.timedelta(minutes=self.dashboard_minutes_before)
        ts_to = start + datetime.timedelta(minutes=self.dashboard_minutes_after)

        ts_from = int(time.mktime(ts_from.timetuple()))
        ts_to = int(time.mktime(ts_to.timetuple()))

        return f'{self.dashboard_url_prefix}from={ts_from}000&to={ts_to}000&var-Host={target}'

    def _read_sample(self):
        session, engine = set_up_db(self.db_config.__dict__)

        try:
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
        finally:
            session.close()
            engine.dispose()

    def _start_incidents(self, anomalies):
        if anomalies is None:
            return

        if self.incidents is None:
            new_incidents = anomalies
        else:
            new_incidents = pd.merge(anomalies, self.incidents[['target']], how='outer', indicator=True)
            new_incidents = new_incidents[new_incidents['_merge'] == 'left_only']
            new_incidents = new_incidents.drop('_merge', 1)

        new_incidents = new_incidents[new_incidents['traffic'] > self.min_traffic_incident]
        if new_incidents.empty:
            return

        # save the new incidents
        new_incidents['id'] = 0
        new_incidents['start'] = pd.to_datetime(new_incidents['time'], unit='s', utc=True)
        new_incidents = new_incidents.drop('time', 1)

        session, engine = set_up_db(self.db_config.__dict__)

        try:
            for index, row in new_incidents.iterrows():
                start = row['start'].strftime('%Y-%m-%d %H:%M:%SZ')
                attack = Attack()
                attack.start = start
                attack.target = row['target']
                attack.detected_traffic = row['traffic']
                attack.anomaly_traffic_portion = row['challenged_portion']
                dashboard_url = self._get_dashboard_url(row['start'], row['target'])
                attack.dashboard_url = dashboard_url
                session.add(attack)
                session.commit()
                new_incidents.at[index, 'id'] = attack.id

                target = row['target']
                self.logger.info(f'New incident, target={target}, id={attack.id}, '
                                 f'traffic={row["traffic"]:.0f} ({row["avg_traffic"]:.0f}) '
                                 f'anomaly_portion={row["challenged_portion"]:.2f} '
                                 f'({row["avg_challenged_portion"]:.2f}) '
                                 f'url="{dashboard_url}" '
                                 )
                if self.mail_sender and self.emails:
                    self.mail_sender.send(self.emails,
                                          f'Incident {attack.id}, target = {target}',
                                          f'Baskerville detected a new incident:\n\n'
                                          f'ID = {attack.id}\n'
                                          f'Targeted host = {target}\n'
                                          f'Timestamp = {start}\n\n'
                                          f'Anomaly traffic portion = {attack.anomaly_traffic_portion:.2f}\n'
                                          f'Unique IPs (1st batch) = {attack.detected_traffic:.0f}\n'
                                          f'Dashboard URL : {dashboard_url}'
                                          )

        except Exception as e:
            if self.logger:
                self.logger.error(str(e))
            return
        finally:
            session.close()
            engine.dispose()

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
        session, engine = set_up_db(self.db_config.__dict__)
        try:
            for index, row in stopped_incidents.iterrows():
                session.query(Attack).filter(Attack.id == row['id']).update(
                    {'stop': row['stop'].strftime('%Y-%m-%d %H:%M:%SZ')})
                target = row['target']
                attack_id = row['id']
                self.logger.info(f'Incident finished, target={target}, id = {attack_id}')

            session.commit()

        except Exception as e:
            print(str(e))
            if self.logger:
                self.logger.error(str(e))
            return

        finally:
            session.close()
            engine.dispose()

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
                    (batch['traffic'] > (batch['avg_traffic'] + self.sigma_traffic * batch['stddev_traffic'])) & \
                    (batch['challenged_portion'] > self.min_challenged_portion_incident)

        anomalies = batch[condition]
        regulars = batch[~condition]

        self._stop_incidents(regulars)
        self._start_incidents(anomalies)
