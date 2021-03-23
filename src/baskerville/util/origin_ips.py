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

    def __init__(self, url, url2, logger, refresh_period_in_seconds=300):
        self.url = url
        self.url2 = url2
        self.refresh_period_in_seconds = refresh_period_in_seconds
        self.ips = None
        self.last_timestamp = None
        self.logger = logger
        self.refresh()

    def read_json_from_url(self, url):
        try:
            html = urlopen(url).read()
        except HTTPError as e:
            self.logger.error(f'HTTP Error {e.code} while parsing origin host IPs from {url}')
            return None
        except URLError as e:
            self.logger.error(f'URL error {e.reason} while getting origin host IPs from {url}')
            return None

        try:
            data = json.loads(html)
        except json.JSONDecodeError as e:
            self.logger.error(f'JSON error {e} while getting origin host IPs from {url}')
            return None

        return data

    def refresh(self):
        if not self.last_timestamp or int(time.time() - self.last_timestamp) > self.refresh_period_in_seconds:
            self.last_timestamp = time.time()
            self.ips = []
            self.logger.info('Refreshing origin IPs...')
            data = self.read_json_from_url(self.url)
            if data:
                self.ips = list(data.values())

            self.logger.info('Refreshing origin IPs 2 ...')
            data = self.read_json_from_url(self.url)
            if data:
                for k, v in data.items():
                    self.ips += v

    def get(self):
        if not self.url:
            return []

        self.refresh()
        return self.ips
