# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import json

from confluent_kafka import Consumer


def process(batch_msgs):
    import pandas as pd

    batch_msgs = [json.loads(b.value(), encoding='utf-8')
                  for b in batch_msgs if b]
    df = pd.DataFrame(batch_msgs)
    print(df.head())


if __name__ == '__main__':
    c = Consumer(**{'bootstrap.servers': '0.0.0.0:9092', 'group.id': 'mygroup',
                    'default.topic.config': {'auto.offset.reset': 'smallest'}})
    c.subscribe(['ats.logs'])
    running = True

    while running:
        msgs = c.consume(num_messages=100, timeout=15)
        print(len(msgs))
        process(msgs)

    c.close()
