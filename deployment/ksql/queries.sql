CREATE TABLE STATS_5M  AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
sum (reply_length_bytes) as traffic,
sum (cached*reply_length_bytes) as traffic_cached,
count (*) as requests,
sum(cached) as requests_cached,
COLLECT_LIST (client_ip) as ips,
HISTOGRAMM(country) as countries,
HISTOGRAM (client_url) as urls,
HISTOGRAM (ua_name) as ua,
HISTOGRAM (http_response_code) as status_code,
TIMESTAMPTOSTRING(WINDOWSTART, 'yyy-MM-dd HH:mm:ss', 'UTC') as ts_window_start,
TIMESTAMPTOSTRING(WINDOWEND, 'yyy-MM-dd HH:mm:ss', 'UTC') as ts_window_end
 FROM STATS_WEBLOGS
 WINDOW TUMBLING (SIZE 5 MINUTES)
 GROUP BY client_request_host;



--CREATE STREAM STATS_WEBLOGS_SCHEMA (
--    client_request_host VARCHAR,
--    client_ip VARCHAR,
--    client_url VARCHAR,
--    ua_name VARCHAR,
--    http_response_code VARCHAR,
--    timestamp VARCHAR,
--    reply_length_bytes BIGINT,
--    geoip STRUCT<country_name VARCHAR>,
--    cache_result VARCHAR
--) WITH (
--    kafka_topic = 'deflect.logs',
--    partitions = 3,
--    value_format = 'json',
--    timestamp = 'timestamp',
--    timestamp_format = 'yyyy-MM-dd''T''HH:mm:ss.SSSZ'
--);

CREATE STREAM STATS_WEBLOGS_SCHEMA (
    client_request_host VARCHAR,
    client_ip VARCHAR,
    client_url VARCHAR,
    ua_name VARCHAR,
    http_response_code VARCHAR,
    ts_timestamp VARCHAR,
    reply_length_bytes BIGINT,
    geoip STRUCT<country_name VARCHAR>,
    cache_result VARCHAR
) WITH (
    kafka_topic = 'deflect.logs',
    partitions = 3,
    value_format = 'json',
    timestamp = 'ts_timestamp',
    timestamp_format = 'dd/LLL/yyyy:HH:mm:ss ZZZ'
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









CREATE TABLE STATS_TEST3 AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
count (*) as total,
sum(cached) as cached,
HISTOGRAM(country) as countries
 FROM STATS_WEBLOGS
 WINDOW HOPPING (SIZE 5 MINUTE, ADVANCE BY 1 minute)
 where client_request_host = 'kavkaz-uzel.media'
 GROUP BY client_request_host;


CREATE TABLE STATS_TEST7 AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
LATEST_BY_OFFSET(ts_timestamp) as ts_timestamp,
count (*) as total,
sum(cached) as cached,
WINDOWSTART AS window_start,
WINDOWEND AS window_end,
TIMESTAMPTOSTRING(WINDOWSTART, 'yyy-MM-dd HH:mm:ss', 'UTC') as ts_window_start,
TIMESTAMPTOSTRING(WINDOWEND, 'yyy-MM-dd HH:mm:ss', 'UTC') as ts_window_end,
HISTOGRAM(country) as countries
 FROM STATS_WEBLOGS
 WINDOW TUMBLING (SIZE 5 MINUTE)
 where client_request_host = 'kavkaz-uzel.media'
 GROUP BY client_request_host;



CREATE STREAM STATS_TEST5 WITH
(
  timestamp = ts_timestamp
) AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
count (*) as total,
sum(cached) as cached,
HISTOGRAM(country) as countries
 FROM STATS_WEBLOGS
 WINDOW TUMBLING (SIZE 5 MINUTE)
 where client_request_host = 'kavkaz-uzel.media'
 GROUP BY client_request_host;



CREATE TABLE STATS_TUMBLING_REQUESTS_5M_2 AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
count (*) as total,
sum(cached) as cached,
HISTOGRAM(country) as countries
 FROM STATS_WEBLOGS
 WINDOW TUMBLING (SIZE 5 MINUTES)
 GROUP BY client_request_host;







CREATE TABLE STATS_HOPING_REQUESTS_1H AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
count (*) as total,
sum(cached) as cached
 FROM STATS_WEBLOGS
 WINDOW HOPPING (SIZE 60 MINUTES, ADVANCE BY 60 SECONDS)
 GROUP BY client_request_host;


CREATE TABLE STATS_HOPING_REQUESTS_TEST3 AS
SELECT client_request_host, EARLIEST_BY_OFFSET(client_request_host) as host,
count (*) as total,
sum(cached) as cached
 FROM STATS_WEBLOGS
 WINDOW HOPPING (SIZE 60 SECONDS, ADVANCE BY 20 SECONDS)
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

CREATE STREAM STATS_BANJAX_SCHEMA (
    http_host VARCHAR,
    client_ip VARCHAR,
    action VARCHAR,
    uripath VARCHAR,
    user_agent STRUCT<name VARCHAR>,
    geoip STRUCT<country_name VARCHAR>,
    "datestamp" VARCHAR
) WITH (
    kafka_topic = 'banjax',
    partitions = 3,
    value_format = 'json',
    timestamp = "datestamp",
    timestamp_format = 'yyyy-MM-dd''T''HH:mm:ss'
);


CREATE STREAM STATS_BANJAX AS
    SELECT
        http_host,
        client_ip,
        uripath,
        user_agent->name as ua_name,
        geoip->country_name as country
    FROM STATS_BANJAX_SCHEMA
    PARTITION BY http_host;


CREATE TABLE STATS_BAN AS
SELECT http_host, EARLIEST_BY_OFFSET(http_host) as host,
count (*) as total
 FROM STATS_BANJAX
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