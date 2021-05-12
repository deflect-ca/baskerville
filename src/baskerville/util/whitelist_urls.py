# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from baskerville.util.json_url_reader import JsonUrlReader


class WhitelistURLs(object):

    def __init__(self, url, logger, refresh_period_in_seconds=300):
        self.reader = JsonUrlReader(url=url, logger=logger, refresh_period_in_seconds=refresh_period_in_seconds)
        self.logger = logger

    def get(self):
        urls = []
        data = self.reader.get()
        if data:
            urls = list(set(data['white_list_urls']))

        self.logger.info(f'Whitelist urls = {urls}')
        return urls
