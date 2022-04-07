prefix = "STATS_"

postfixes = [
    ('5M', '5 MINUTES'),
    ('30M', '30 MINUTES'),
    ('1H', '60 MINUTES'),
    ('6H', '6 HOURS'),
    ('1D', '24 HOURS'),
    ('1w', '7 DAYS')
]

schemas = [
    f"""
    CREATE STREAM WEBLOGS_SCHEMA3 (
    client_request_host VARCHAR,
    client_ip VARCHAR,
    client_url VARCHAR,
    ua_name VARCHAR,
    http_response_code VARCHAR,
    timestamp VARCHAR,
    reply_length_bytes BIGINT,
    geoip STRUCT<country_name VARCHAR>,
    cache_result VARCHAR
) WITH (
    kafka_topic = 'deflect.logs',
    partitions = 3,
    value_format = 'json',
    timestamp = 'timestamp',
    timestamp_format = 'yyyy-MM-dd''T''HH:mm:ssZ'
);
"""
]