# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from urllib.error import HTTPError, URLError
from urllib.request import urlopen
import json
import time


class JsonUrlReader(object):
    def __init__(self, url, logger=None, refresh_period_in_seconds=300):
        self.url = url
        self.refresh_period_in_seconds = refresh_period_in_seconds
        self.last_timestamp = None
        self.logger = logger
        self.data = None
        self.refresh()

    def read_json_from_url(self, url):
        try:
            html = urlopen(url).read()
        except HTTPError as e:
            if self.logger:
                self.logger.error(f'HTTP Error {e.code} while parsing url {url}')
            return None
        except URLError as e:
            if self.logger:
                self.logger.error(f'URL error {e.reason} while getting from {url}')
            return None

        try:
            data = json.loads(html)
        except json.JSONDecodeError as e:
            if self.logger:
                self.logger.error(f'JSON error {e} while getting from {url}')
            return None

        return data

    def refresh(self):
        if not self.url:
            return
        if not self.last_timestamp or int(time.time() - self.last_timestamp) > self.refresh_period_in_seconds:
            self.last_timestamp = time.time()
            if self.url:
                if self.logger:
                    self.logger.info(f'Refreshing from url {self.url}...')
                self.data = self.read_json_from_url(self.url)

    def get(self):
        if not self.url:
            return None

        self.refresh()
        return self.data


