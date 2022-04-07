CREATE STREAM STATS_WEBLOGS_SCHEMA (
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

CREATE STREAM STATS_WEBLOGS AS
    SELECT
        client_request_host,
        client_url,
        ts_timestamp,
        reply_length_bytes,
        geoip->country_name as country,
        client_ip,
        ua_name,
        http_response_code,
        CASE
            WHEN cache_result <> 'TCP_MISS' THEN 1
         ELSE 0
        END AS cached
    FROM STATS_WEBLOGS_SCHEMA
    PARTITION BY client_request_host;








CREATE TABLE REQUESTS9 AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
count (*) as total,
sum(cached) as cached,
HISTOGRAM(country) as countries
 FROM WEBLOGS9
 WINDOW TUMBLING (SIZE 5 MINUTES)
 GROUP BY client_request_host;

CREATE TABLE CACHE_HITS AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
count (*) as total,
sum(cached) as cached
 FROM WEBLOGS9
 WINDOW TUMBLING (SIZE 7 DAYS)
 GROUP BY client_request_host;


CREATE TABLE TRAFFIC3 AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
sum (reply_length_bytes) as total_traffic,
sum (cached*reply_length_bytes) as cached_traffic
 FROM WEBLOGS6
 WINDOW TUMBLING (SIZE 5 MINUTES)
 GROUP BY client_request_host;


CREATE TABLE UNIQUE_IPS AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
COUNT_DISTINCT (client_ip) as ip
 FROM WEBLOGS7
 WINDOW TUMBLING (SIZE 5 MINUTES)
 GROUP BY client_request_host;

CREATE TABLE COUNTRIES AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
HISTOGRAM (country) as countries
 FROM WEBLOGS7
 WINDOW TUMBLING (SIZE 5 MINUTES)
 GROUP BY client_request_host;

CREATE TABLE URLS_1h AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
HISTOGRAM (client_url) as urls
 FROM WEBLOGS8
 WINDOW TUMBLING (SIZE 60 MINUTES)
 GROUP BY client_request_host;

CREATE TABLE USER_AGENT AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
HISTOGRAM (ua_name) as ua_name
 FROM WEBLOGS9
 WINDOW TUMBLING (SIZE 5 MINUTES)
 GROUP BY client_request_host;

CREATE TABLE STATUS_CODE AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
HISTOGRAM (http_response_code) as status_code
 FROM WEBLOGS9
 WINDOW TUMBLING (SIZE 5 MINUTES)
 GROUP BY client_request_host;

 ----------------------------------

CREATE STREAM BANJAX_SCHEMA (
    http_host VARCHAR,
    client_ip VARCHAR,
    action VARCHAR,
    uripath VARCHAR,
    user_agent STRUCT<name VARCHAR>,
    geoip STRUCT<country_name VARCHAR>,
    timestamp VARCHAR
) WITH (
    kafka_topic = 'banjax',
    partitions = 3,
    value_format = 'json',
    timestamp = 'timestamp',
    timestamp_format = 'yyyy-MM-dd''T''HH:mm:ssZ'
);

CREATE STREAM BANJAX AS
    SELECT
        http_host,
        client_ip,
        action,
        uripath,
        user_agent->name as ua_name,
        geoip->country_name as country
    FROM BANJAX_SCHEMA
    PARTITION BY http_host;


CREATE TABLE BAN AS
SELECT http_host, EARLIEST_BY_OFFSET(http_host) as host,
count (*) as total
 FROM BANJAX
 WINDOW TUMBLING (SIZE 5 MINUTES)
 GROUP BY http_host;

CREATE TABLE BAN_COUNTRIES AS
SELECT http_host, EARLIEST_BY_OFFSET(http_host) as host,
HISTOGRAM (country) as countries
 FROM BANJAX
 WINDOW TUMBLING (SIZE 5 MINUTES)
 GROUP BY http_host;

 CREATE TABLE BAN_URLS_24H AS
SELECT http_host, EARLIEST_BY_OFFSET(http_host) as host,
HISTOGRAM (uripath) as urls
 FROM BANJAX
 WINDOW TUMBLING (SIZE 24 HOURS)
 GROUP BY http_host;

 -------------------------------------------------------------------------------
 CREATE TABLE HOP_COUNTRIES1 AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
HISTOGRAM (country) as countries
 FROM WEBLOGS7
 where client_request_host = 'kavkaz-uzel.eu'
 WINDOW HOPPING (SIZE 1 HOUR, ADVANCE BY 60 SECONDS)
 GROUP BY client_request_host;