# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

from baskerville.util.json_url_reader import JsonUrlReader


class WhitelistIPs(object):

    def __init__(self, url, url2, logger=None, refresh_period_in_seconds=300):
        self.reader1 = JsonUrlReader(url=url, logger=logger, refresh_period_in_seconds=refresh_period_in_seconds)
        self.reader2 = JsonUrlReader(url=url2, logger=logger, refresh_period_in_seconds=refresh_period_in_seconds)
        self.ips = None
        self.logger = logger

    def get_global_ips(self):
        ips = []
        data = self.reader2.get()
        if data:
            if 'global' in data:
                for ip in data['global']:
                    ips.append(ip.split('/')[0])

        ips = list(set(ips))
        return ips

    def get_host_ips(self):
        host_ips = {}
        data1 = self.reader1.get()
        if data1:
            for k, v in data1.items():
                host_ips[k] = [v]

        data2 = self.reader2.get()
        if data2:
            for k, v in data2.items():
                if k == 'global' or k == 'global_updated_at':
                    continue
                ips = []
                for ip in v:
                    ips.append(ip.split('/')[0])
                ips = list(set(ips))
                if k in host_ips:
                    host_ips[k] += ips
                else:
                    host_ips[k] = ips
        return host_ips
