import json

from kafka import KafkaProducer, KafkaConsumer
from pyspark.sql import functions as F, types as T


def send_partition_to_kafka(connection, client_connections):
    def f(topic, cc_to_client, client_topic, send_id_client, rows):
        try:
            from kafka import KafkaProducer
            producer = None
            if connection.value:
                producer = KafkaProducer(**connection.value)

            client_producers = {}
            if cc_to_client and client_connections.value:
                for k, v in client_connections.value.items():
                    try:
                        client_producers[k] = KafkaProducer(**v)
                    except Exception:
                        pass

            for row in rows:
                row = row.asDict()
                if send_id_client:
                    id_client = row.get('id_client', None)
                else:
                    id_client = row.pop('id_client', None)

                message = json.dumps(row).encode('utf-8')
                if producer:
                    producer.send(topic, message)
                if cc_to_client and id_client:
                    client_producer = client_producers.get(id_client, None)
                    if client_producer:
                        client_producer.send(client_topic, message)

            producer.flush()
            for _, client_producer in client_producers.items():
                client_producer.flush()

        except Exception:
            import traceback
            traceback.print_exc()
            return False
        return True

    return F.udf(f, T.BooleanType())


def send_to_kafka(spark,
                  df,
                  columns,
                  topic,
                  connection,
                  cc_to_client=False,
                  client_topic=None,
                  client_connections=None,
                  use_partitions=True,
                  logger=None):
    if use_partitions:
        broadcast_connection = spark.sparkContext.broadcast(connection)
        broadcast_client_connections = spark.sparkContext.broadcast(client_connections)

        send_id_client = True
        if cc_to_client and 'id_client' not in columns:
            columns.append('id_client')
            send_id_client = False

        df = df.select(
            F.struct(
                *list(
                    F.col(c) for c in columns
                )).alias('rows'),
            F.spark_partition_id().alias('pid')
        ).cache()

        f_ = F.collect_list('rows')
        g_records = df.groupBy('pid').agg(f_.alias('rows')).cache()
        g_records = g_records.withColumn(
            'sent_to_kafka',
            send_partition_to_kafka(
                broadcast_connection,
                broadcast_client_connections
            )
                (
                F.lit(topic),
                F.lit(cc_to_client),
                F.lit(client_topic),
                F.lit(send_id_client),
                F.col('rows'),
            )
        )
        # broadcast_connection.destroy()
        # broadcast_client_connections.destroy()

        # False means something went wrong:
        print(g_records.select('*').where(
            F.col('sent_to_kafka') == False  # noqa
        ).head(1))
        return g_records
    else:
        producer = KafkaProducer(**connection)
        client_producers = None
        if cc_to_client:
            client_producers = {}
            for client, client_connection in client_connections.items():
                client_producers[client] = KafkaProducer(**client_connection)

        records = df.collect()
        for record in records:
            message = json.dumps(
                {key: record[key] for key in columns}
            ).encode('utf-8')

            producer.send(topic, message)

            if cc_to_client:
                id_client = record['id_client']
                if id_client and id_client in client_producers:
                    client_producers[id_client].send(client_topic, message)

        producer.flush()
        if client_producers:
            for _, client_producer in client_producers.items():
                client_producer.flush()


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
