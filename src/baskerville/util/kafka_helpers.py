import json

from kafka import KafkaProducer, KafkaConsumer


def send_to_kafka(df, columns, bootstrap_servers, topic, cc_to_client=False):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    records = df.collect()
    for record in records:
        message = json.dumps(
            {key: record[key] for key in columns}
        ).encode('utf-8')
        producer.send(topic, message)
        if cc_to_client:
            id_client = record['id_client']
            producer.send(f'{topic}.{id_client}', message)
    producer.flush()


def read_from_kafka_from_the_beginning(bootstrap_servers, topic, schema, spark):
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=3000,
    )
    data = []
    for message in consumer:
        data.append([message.value.decode("utf-8")])
    df = spark.createDataFrame(data, ['data'])
    df = df.rdd.map(lambda l: json.loads(l[0])).toDF(schema)
    return df
