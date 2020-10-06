# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import os
import pickle
import threading

from cachetools import TTLCache

from baskerville.util.helpers import get_default_ip_cache_path
from baskerville.util.singleton_thread_safe import SingletonThreadSafe


class IPCache(metaclass=SingletonThreadSafe):

    def __init__(self, config, logger):
        super().__init__()

        self.logger = logger
        self.lock = threading.Lock()

        folder_path = get_default_ip_cache_path()
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)

        self.full_path_passed_challenge = os.path.join(folder_path, 'ip_cache_passed_challenge.bin')
        if os.path.exists(self.full_path_passed_challenge):
            self.logger.info(f'Loading passed challenge IP cache from file {self.full_path_passed_challenge}...')
            with open(self.full_path_passed_challenge, 'rb') as f:
                self.cache_passed = pickle.load(f)
        else:
            self.cache_passed = TTLCache(
                maxsize=config.engine.ip_cache_passed_challenge_size,
                ttl=config.engine.ip_cache_passed_challenge_ttl)
            self.logger.info('A new instance of passed challege IP cache has been created')

        self.full_path_pending = os.path.join(folder_path, 'ip_cache_pending.bin')
        if os.path.exists(self.full_path_pending):
            self.logger.info(f'Loading passed challenge IP cache from file {self.full_path_pending}...')
            with open(self.full_path_pending, 'rb') as f:
                self.cache_pending = pickle.load(f)
        else:
            self.cache_pending = TTLCache(
                maxsize=config.engine.ip_cache_pending_size,
                ttl=config.engine.ip_cache_pending_ttl)
            self.logger.info('A new instance of pending IP cache has been created')

    def update(self, records):
        with self.lock:
            self.logger.info('IP cache updating...')
            if len(self.cache_passed) > 0.98 * self.cache_passed.maxsize:
                self.logger.warning('IP cache passed challenge is 98% full. ')
            if len(self.cache_pending) > 0.98 * self.cache_pending.maxsize:
                self.logger.warning('IP cache pending challenge is 98% full. ')
            result = []
            for r in records:
                if r['ip'] not in self.cache_passed and r['ip'] not in self.cache_pending:
                    result.append(r)

            for r in result:
                self.cache_pending[r['ip']] = {
                    'fails': 0
                }

            with open(self.full_path_pending, 'wb') as f:
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
                self.logger.info(f'ip: {ip}, fails : {num_fails}')
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

