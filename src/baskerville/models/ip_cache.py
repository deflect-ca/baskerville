# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from cachetools import TTLCache


class IPCache(object):

    def __init__(self, logger, ttl=60 * 60, max_size=100000):
        super().__init__()
        self.cache = TTLCache(maxsize=max_size, ttl=ttl)
        self.logger = logger

    def update(self, records):
        self.logger.info('IP cache updating...')
        if len(self.cache) > 0.98 * self.cache.maxsize:
            raise RuntimeError(
                'IP cache is 98% full. Please increase parameter ip_cache_size or/and reduce ip_cache_ttl')

        result = []
        for r in records:
            if r['ip'] not in self.cache:
                result.append(r)

        for r in result:
            self.cache[r['ip']] = {}

        self.logger.info(
            f'IP cache: {len(self.cache)} total, {len(records) - len(result)} existed, {len(result)} added')

        return result
