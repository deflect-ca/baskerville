CREATE STREAM HITS_SCHEMA (
    client_request_host VARCHAR,
    client_ip VARCHAR,
    ts_timestamp VARCHAR
) WITH (
    kafka_topic = 'deflect.logs',
    partitions = 3,
    value_format = 'json',
    timestamp = 'ts_timestamp',
    timestamp_format = 'dd/LLL/yyyy:HH:mm:ss X'
);

CREATE STREAM HITS AS
    SELECT *
    FROM HITS_SCHEMA
    PARTITION BY client_request_host;

CREATE TABLE REQUESTS_COUNT AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host, count (*) as requests FROM HITS
  WINDOW TUMBLING (SIZE 5 MINUTES)
  GROUP BY client_request_host;


