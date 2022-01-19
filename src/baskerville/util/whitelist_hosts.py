# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from baskerville.util.json_url_reader import JsonUrlReader


class WhitelistHosts(object):

    def __init__(self, url, logger, refresh_period_in_seconds=300):
        self.reader = JsonUrlReader(url=url, logger=logger, refresh_period_in_seconds=refresh_period_in_seconds)
        self.logger = logger

    def get(self):
        hosts = []
        data = self.reader.get()
        if data:
            hosts = list(set(data['white_list_hosts']))

        return hosts
