# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from baskerville.util.json_url_reader import JsonUrlReader


class OriginIPs(object):

    def __init__(self, url, url2, logger, refresh_period_in_seconds=300):
        self.reader1 = JsonUrlReader(url=url, logger=logger, refresh_period_in_seconds=refresh_period_in_seconds)
        self.reader2 = JsonUrlReader(url=url2, logger=logger, refresh_period_in_seconds=refresh_period_in_seconds)
        self.ips = None
        self.logger = logger

    def get(self):
        ips = []
        data1 = self.reader1.get()
        if data1:
            ips = list(data1.values())

        data2 = self.reader2.get()
        if data2:
            for k, v in data2.items():
                for ip in v:
                    ips.append(ip.split('/')[0])
        ips = list(set(ips))
        self.logger.info(f'Origin ips = {ips}')
        return ips

