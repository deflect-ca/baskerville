# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.transport.kafka.message import Message


class BaskervilleProducer(object):
    def __init__(self, owner, kind, kafka_client, extra_attrs=None):
        self.owner = owner
        self.kind = kind
        self.kafka_client = kafka_client
        self.extra_attrs = extra_attrs if extra_attrs else {}
        self.topic_name = self.get_topic_name()
        print(self.topic_name)
        self.topic = self.kafka_client.topics[self.topic_name]
        self.producer = self.topic.get_producer()

    def get_topic_name(self):
        return bytes('{}.{}'.format(self.owner, self.kind).encode('utf-8'))

    def _encode(self, body, encoder=Message):
        """
        Sringify message.
        :param str body: the message body
        :return: the strigified Message
        :rtype: bytes
        """
        return bytes(str(encoder(
            owner=self.owner, body=body, kind=self.kind, **self.extra_attrs
        )).encode('utf-8'))

    def produce(self, message, *args, **kwargs):
        """
        Produce
        :param str message:
        :param partition_key:
        :param timestamp:
        :return:
        """
        return self.producer.produce(
            self._encode(message),
            *args,
            **kwargs
        )


if __name__ == '__main__':
    from pykafka import KafkaClient
    import time

    client = KafkaClient('0.0.0.0:9092', zookeeper_hosts='localhost:2181')
    producer = BaskervilleProducer('ats', 'logs', kafka_client=client)
    while True:
        message = producer.produce('Hello')
        print(message.offset, message.value)
        time.sleep(5)
