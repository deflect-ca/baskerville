# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


from baskerville.transport.kafka.message import Message
from confluent_kafka import Consumer


# http://cloudurable.com/blog/kafka-architecture-consumers/index.html
class BaskervilleConsumer(object):
    def __init__(
            self,
            target_owner,
            kind,
            kafka_client,
            decoder_cls=Message,
            extra_attrs=None,
            *args,
            **kwargs
    ):
        self.owner = target_owner
        self.kind = kind
        self.kafka_client = kafka_client
        self.decoder = decoder_cls(owner=self.owner, kind=self.kind)
        self.extra_attrs = extra_attrs if extra_attrs else {}
        print(self.decoder.get_topic())
        self.topic_name = bytes(self.decoder.get_topic())
        self.topic = self.kafka_client.topics[self.topic_name]
        self.consumer = self.topic.get_simple_consumer(*args, **kwargs)

    def get_topic_name(self):
        return bytes('{}.{}'.format(self.owner, self.kind).encode('utf-8'))

    @staticmethod
    def _decode(message, decoder_cls=Message):
        """

        :param str message: the incoming kafka message
        :param T decoder_cls: the decoder class that provides a load method,
        accepting message as input
        :return: the decoded message of type T
        :rtype: T
        """
        return decoder_cls.load(message)

    def consume(self, decoder_cls=Message, *args, **kwargs):
        """
        Consumes from a Kafka topic and returns the decoded message
        :return: the decoded message
        :rtype: Message
        """
        message = self.consumer.consume(
            *args, **kwargs
        )

        return self._decode(message, decoder_cls=decoder_cls)

    def __iter__(self):
        for message in self.consumer:
            if message:
                yield self._decode(message)


class BaskervilleConfluentKafkaBatchConsumer(object):
    def __init__(
            self,
            target_owner,
            kind,
            bootstrap_servers='0.0.0.0:9092',
            group_id='baskerville',
            offset_reset='smallest',
            decoder_cls=Message,
            extra_attrs=None,
    ):
        self.owner = target_owner
        self.kind = kind
        self.decoder = decoder_cls(owner=self.owner, kind=self.kind)
        self.extra_attrs = extra_attrs if extra_attrs else {}
        self.topic_name = str(self.decoder.get_topic().decode('utf-8'))
        self.consumer = Consumer(
            **{
                'bootstrap.servers': bootstrap_servers,
                'group.id': group_id,
                'default.topic.config': {
                    'auto.offset.reset': offset_reset
                }
            }
        )
        self.consumer.subscribe([self.topic_name])

    def get_topic_name(self):
        return bytes('{}.{}'.format(self.owner, self.kind).encode('utf-8'))

    @staticmethod
    def _decode(message, decoder_cls=Message):
        """

        :param str message: the incoming kafka message
        :param T decoder_cls: the decoder class that provides a load method,
        accepting message as input
        :return: the decoded message of type T
        :rtype: T
        """
        return decoder_cls.load(message)

    def consume(
            self,
            num_messages=1000000,
            timeout=15,
            decoder_cls=Message,
            *args, **kwargs
    ):
        """
        Consumes from a Kafka topic and returns the decoded message
        :return: the decoded message
        :rtype: list[Message]
        """
        messages = self.consumer.consume(
            num_messages=num_messages,
            timeout=timeout,
            *args, **kwargs
        )

        return [self._decode(message, decoder_cls=decoder_cls)
                for message in messages if message]

    def __iter__(self):
        print('consuming')
        yield self.consumer.consume()
        # for message in self.consumer.consume():
        #     if message:
        #         yield self._decode(message)
        #     print('consuming')


if __name__ == '__main__':
    # from pykafka import KafkaClient
    #
    # client = KafkaClient('0.0.0.0:9092', zookeeper_hosts='localhost:2181')
    # consumer = BaskervilleConsumer('ats', 'logs', kafka_client=client)
    # for message in consumer:
    #     print('Hey:', str(message))

    consumer = BaskervilleConfluentKafkaBatchConsumer('ats', 'logs')
    print([str(m) for m in consumer.consume()])
    for message in consumer:
        print('Hey:', str(message))
