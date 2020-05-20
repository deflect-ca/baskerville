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
