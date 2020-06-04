# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import traceback

from attr import dataclass
from prometheus_client import Counter, Gauge, Summary
from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass
class Notifier:
    session: Session
    metrics: dict
    test: dict

    def cmd(self, df, metric_name, func):
        try:
            res = func(df)
            self.test[metric_name].set(float(res))
            return f'NOTIFY {metric_name}, \'{res}\';'
        except Exception:
            traceback.print_exc()

    def notify(self, metric_name, df):
        if metric_name in self.metrics:
            if metric_name not in self.test:
                n = f'baskerville_{metric_name}'
                self.test[metric_name] = Gauge(n, n, None)
            cmd = self.cmd(df, metric_name, self.metrics[metric_name])
            if cmd:
                self.session.execute(
                    text(cmd).execution_options(autocommit=True)
                )
                # print(f'EXECUTED {cmd} for {metric_name} with result {res}')


class Collector(object):
    def __init__(self):
        pass

    def collect(self, metric_name, value):
        if metric_name in self.metrics:
            curr_metric = self.metrics[metric_name]
            if isinstance(curr_metric, Counter):
                curr_metric.inc(value)
            elif isinstance(curr_metric, Gauge):
                curr_metric.set(value)
            elif isinstance(curr_metric, Summary):
                curr_metric.inc(value)
            else:
                print('meh...')
