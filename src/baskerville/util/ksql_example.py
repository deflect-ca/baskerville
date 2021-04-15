import logging
from ksql import KSQLAPI
logging.basicConfig(level=logging.DEBUG)
client = KSQLAPI('http://0.0.0.0:8088')

df = None
table_name = 'sensitive_data'
topic = 'predictions'
column_type = [
    'uuid_request_set bigint','ip varchar','target varchar', 'stop varchar'
]
print(client.ksql('show tables'))
client.create_stream(table_name, column_type, topic)
print(client.query(f'select * from {table_name}', use_http2=True))
print(client.ksql('show tables'))

# client.create_stream_as(table_name='sensitive_data',
#                         select_columns=df.columns,
#                         src_table=src_table,
#                         kafka_topic='id_client.sensitive_data',
#                         value_format='json',
#                         conditions=conditions,
#                         partition_by='target',
#                         )
