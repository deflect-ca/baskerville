# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
import datetime
import os
import _pickle as pickle
import threading

from cachetools import TTLCache

from baskerville.util.helpers import get_default_ip_cache_path
from baskerville.util.singleton_thread_safe import SingletonThreadSafe
from pyspark.sql import functions as F


class IPCache(metaclass=SingletonThreadSafe):

    def init_cache(self, path, name, size, ttl):
        if os.path.exists(path):
            with open(path, 'rb') as f:
                result = pickle.load(f)
            self.logger.info(f'Loaded {name} IP cache from file {path}...')
        else:
            result = TTLCache(maxsize=size, ttl=ttl)
            self.logger.info(f'A new instance of {name} IP cache has been created')
        return result

    def __init__(self, config, logger):
        super().__init__()

        self.logger = logger
        self.lock = threading.Lock()

        folder_path = get_default_ip_cache_path()
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)

        self.full_path_passed_challenge = os.path.join(folder_path, 'ip_cache_passed_challenge.bin')
        self.cache_passed = self.init_cache(
            self.full_path_passed_challenge,
            'passed challenge',
            config.engine.ip_cache_passed_challenge_size,
            config.engine.ip_cache_passed_challenge_ttl
        )

        self.full_path_pending_challenge = os.path.join(folder_path, 'ip_cache_pending.bin')
        self.cache_pending = self.init_cache(
            self.full_path_pending_challenge,
            'pending challenge',
            config.engine.ip_cache_pending_size,
            config.engine.ip_cache_pending_ttl
        )

    def update(self, ips):
        """
        Filter new records to find a subset with previously unseen IPs.
        Add the previously unseen IPs values to the cache.
        Return only the subset of previously unseen ips.
        :param ips: a list of ips.
        :return: the subset of previously unseen ips
        """
        with self.lock:
            self.logger.info('IP cache updating...')
            if len(self.cache_passed) > 0.98 * self.cache_passed.maxsize:
                self.logger.warning('IP cache passed challenge is 98% full. ')
            if len(self.cache_pending) > 0.98 * self.cache_pending.maxsize:
                self.logger.warning('IP cache pending challenge is 98% full. ')
            result = []
            for ip in ips:
                if ip not in self.cache_passed and ip not in self.cache_pending:
                    result.append(ip)

            for ip in result:
                self.cache_pending[ip] = {
                    'fails': 0
                }

            with open(self.full_path_pending_challenge, 'wb') as f:
                pickle.dump(self.cache_pending, f)
            self.logger.info(f'IP cache pending: {len(self.cache_pending)}, {len(result)} added')

            return result

    def ip_failed_challenge(self, ip):
        with self.lock:
            if ip not in self.cache_pending.keys():
                return 0

            try:
                value = self.cache_pending[ip]
                value['fails'] += 1
                num_fails = value['fails']
                self.cache_pending['ip'] = value
                return num_fails

            except KeyError as er:
                self.logger.info(f'IP cache key error {er}')
                pass

    def ip_passed_challenge(self, ip):
        with self.lock:
            if ip in self.cache_passed.keys():
                return False
            if ip not in self.cache_pending.keys():
                return False
            self.cache_passed[ip] = self.cache_pending[ip]
            del self.cache_pending[ip]
            self.logger.info(f'IP {ip} passed challenge. Total IP in cache_passed: {len(self.cache_passed)}')

            with open(self.full_path_passed_challenge, 'wb') as f:
                pickle.dump(self.cache_passed, f)
            self.logger.info(f'IP cache passed: {len(self.cache_passed)}, 1 added')
        return True

    def ip_banned(self, ip):
        with self.lock:
            try:
                del self.cache_pending[ip]

            except KeyError as er:
                self.logger.info(f'IP cache key error {er}')
                pass

    def get_time_filter(self):
        return (datetime.datetime.utcnow() - datetime.timedelta(
            minutes=self.config.engine.banjax_sql_update_filter_minutes)).strftime("%Y-%m-%d %H:%M:%S %z")

    def update_passed_challenge(self, df, spark, db_session, delay_seconds=240):
        with self.lock:
            df_pending = spark.createDataFrame([[ip] for ip in self.ip_cache.cache_pending.keys()], ['ip'])
            df_passed = df_pending.join(df.select('ip', 'stop'), on='ip', how='inner') \
                .withColumn('now', F.current_timestamp()) \
                .withColumn('delta', F.unix_timestamp('now') - F.unix_timestamp('stop')) \
                .filter(F.col('delta') > delay_seconds)
            ips = [row['ip'] for row in df_passed['ip'].collect()]
            for ip in ips:
                self.cache_passed[ip] = self.cache_pending[ip]
                del self.cache_pending[ip]

            with open(self.full_path_passed_challenge, 'wb') as f:
                pickle.dump(self.cache_passed, f)

            self.logger.info(df_passed.show())
            self.logger.info('Updating passed in the database...')
            try:
                ips_string = '\', \''.join(ip for ip in ips)
                sql = f'update request_sets set banned = 1 where ' \
                      f'stop > \'{self.get_time_filter()}\' and challenged = 1 ' \
                      f'and ip in \'{ips_string}\''
                db_session.execute(sql)
                db_session.commit()

            except Exception:
                db_session.rollback()
                self.logger.error(Exception)
                raise
            self.logger.info('Updating passed complete.')
