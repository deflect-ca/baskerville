# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from pykafka import KafkaClient


def consume(topic_name):
    client = KafkaClient()
    print(client.topics)
    topic = client.topics[bytes(topic_name)]
    consumer = topic.get_simple_consumer()

    for message in consumer:
        if message:
            print(message.offset, message.value)


if __name__ == '__main__':
    topic_name = 'ats.logs'.encode('utf-8')
    consume(topic_name)
