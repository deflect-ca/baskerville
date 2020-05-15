import json

try:
    from pykafka.protocol import Message as KafkaMessage
except ImportError:
    pass
try:
    from confluent_kafka import cimpl
except ImportError:
    pass
from baskerville.util.helpers import caller_info


class Message(object):
    @caller_info
    def __init__(
            self,
            owner=__name__,
            kind='uncategorized',
            body='{{MISSING}}',
            action=None,
            message_decoder_cls=None,
            encoding='utf-8',
            extra_attrs=None,
            offset=None,
            msg_type=KafkaMessage
    ):
        """
        Message wrapper for DeflectKafka
        :param str owner: the message owner
        :param str kind: the message kind, e.g. logs
        :param T action: CRUD
        :param T body: the message to be sent
        """
        self.offset = offset
        self.owner = owner
        self.body = body
        self.kind = kind
        self.action = action
        self.encoding = encoding
        self.extra_attrs = extra_attrs
        self.msg_type = msg_type
        self.message_decoder_cls = message_decoder_cls
        self.excluded_attrs = [
            'extra_attrs',
            'message_decoder_cls',
            'excluded_attrs',
            'encoding'
        ]

    def __repr__(self):
        return '{s.__class__.__name__}, {s.body}'.format(s=self)

    def get_topic(self):
        """
        Constructs the topic for the current message
        :return: the current message's topic
        :rtype: bytes
        """
        return '{owner}.{kind}'.format(
            owner=self.owner,
            kind=self.kind
        ).encode(self.encoding)

    def message_attributes(self, extra_excluded_attrs=()):
        """
        Returs a list with the message attribute names
        :param tuple extra_excluded_attrs: any extra attributes to exclude
        :return: A list with the message attribute names
        :rtype: list[str]
        """

        attrs_to_exclude = list(extra_excluded_attrs) + self.excluded_attrs

        return [attr
                for attr in dir(self)
                if attr not in attrs_to_exclude
                and not attr.startswith('__')
                and not callable(getattr(self, attr))]

    def to_dict(self):
        """
        Returns self to dict, ignoring magic methods
        :return:
        """
        return {
            attr: getattr(self, attr)
            for attr in self.message_attributes()
        }

    def __str__(self):
        return json.dumps(self.to_dict())

    @classmethod
    def load(cls, message):
        """
        Given a serialized Message, parse and load the parameters
        :param pykafka.protocol.Message message: the serialized Message
        :return: self
        :rtype: baskerville.transport.kafka.message.Message
        """

        if isinstance(message, KafkaMessage):
            value = message.value
            offset = message.offset
        elif isinstance(message, cimpl.Message):
            value = message.value()
            offset = message.offset()
        elif isinstance(message, str):
            value = message
            offset = None
        else:
            raise ValueError('Wrong type of message:{}'.format(type(message)))
        try:
            msg = {'body': json.loads(value), }
            msg['body']['offset'] = offset

            return cls(
                **{
                    attr: msg.get(attr)
                    for attr in cls.message_attributes(cls())
                }
            )
        except AttributeError:
            raise AttributeError('Could not decode message {}'.format(
                message.value
            ))
