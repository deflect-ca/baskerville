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
        self.full_path = os.path.join(folder_path, 'ip_cache.bin')

        if os.path.exists(self.full_path):
            self.logger.info(f'Loading IP cache from file {self.full_path}...')
            with open(self.full_path, 'rb') as f:
                self.cache = pickle.load(f)
            self.logger.info(f'IP cache has been loaded from file {self.full_path}. Size:{len(self.cache)}')
        else:
            self.cache = TTLCache(maxsize=config.engine.ip_cache_size, ttl=config.engine.ip_cache_ttl)
            self.logger.info('A new instance of IP cache has been created')

    def update(self, records):
        with self.lock:
            self.logger.info('IP cache updating...')
            if len(self.cache) > 0.98 * self.cache.maxsize:
                self.logger.warning(
                    'IP cache is 98% full. Please increase parameter ip_cache_size or/and reduce ip_cache_ttl')

            result = []
            for r in records:
                if r['ip'] not in self.cache:
                    result.append(r)

            for r in result:
                self.cache[r['ip']] = {
                    'fails': 0
                }

            with open(self.full_path, 'wb') as f:
                pickle.dump(self.cache, f)

            self.logger.info(
                f'IP cache: {len(self.cache)} total, {len(records) - len(result)} existed, {len(result)} added')

            return result

    def exists(self, ip):
        with self.lock:
            return ip in self.cache.keys()

    def ip_failed_challenge(self, ip):
        with self.lock:
            if ip not in self.cache.keys():
                self.logger.info(f'ip {ip} is not in cache')
                return

            try:
                self.logger.info(f'ip {ip} is in cache')
                value = self.cache[ip]
                value['fails'] += 1
                num_fails = value['fails']
                self.logger.info(f'ip: {ip}, fails : {num_fails}')
                if value['fails'] >= 10:
                    self.logger.info(f'@@@@ ip {ip} has reached 10 fails - banning')
                self.cache['ip'] = value

            except KeyError as er:
                self.logger.info(f'IP cache key error {er}')
                pass
