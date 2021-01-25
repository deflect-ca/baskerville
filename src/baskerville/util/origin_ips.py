# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
import json
import time

class OriginIPs(object):

    def __init__(self, url, logger, refresh_period_in_seconds = 300):
        self.url = url
        self.refresh_period_in_seconds = refresh_period_in_seconds
        self.ips = None
        self.last_timestamp = None
        self.logger = logger

    def refresh(self):
        self.logger.info('Refreshing origin IPs...')
        try:
            html = urlopen(self.url).read()
        except HTTPError as e:
            self.logger.error(f'HTTP Error {e.code} while parsing origin host IPs from {self.url}')
            return
        except URLError as e:
            self.logger.error(f'URL error {e.reason} while getting origin host IPs from {self.url}')
            return

        try:
            hosts = json.loads(html)
        except json.JSONDecodeError as e:
            self.logger.error(f'JSON error {e} while getting origin host IPs from {self.url}')
            return

        self.ips = list(hosts.values())

    def get(self):
        if not self.ips or int(time.time() - self.last_timestamp) > self.refresh_period_in_seconds:
            self.last_timestamp = time.time()
            self.refresh()

        return self.ips
