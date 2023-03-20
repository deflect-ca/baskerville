# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import threading

from cachetools import TTLCache
from baskerville.util.singleton_thread_safe import SingletonThreadSafe


class IPCache(metaclass=SingletonThreadSafe):

    def __init__(self, config, logger):
        super().__init__()

        self.logger = logger
        self.lock = threading.Lock()

        self.cache_passed = TTLCache(
            config.engine.ip_cache_passed_challenge_size,
            config.engine.ip_cache_passed_challenge_ttl
        )

        self.cache_pending = TTLCache(
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
            for ip, target, low_rate_attack in ips:
                if ip not in self.cache_passed and ip not in self.cache_pending:
                    result.append((ip, target, low_rate_attack))

            for ip, _, _ in result:
                self.cache_pending[ip] = {
                    'fails': 0
                }

            return result

    def ip_failed_challenge(self, ip):
        with self.lock:
            if ip not in self.cache_pending.keys():
                return

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
                return
            self.cache_passed[ip] = 1

    def ip_banned(self, ip):
        pass
