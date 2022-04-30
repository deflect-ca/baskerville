prefix = "STATS_"


tumbling_windows = [
    ('_5M', '5 MINUTES'),
    ('_30M', '30 MINUTES'),
    ('_1H', '60 MINUTES'),
    ('_1D', '24 HOURS'),
    ('_1W', '7 DAYS')
]

hopping_windows = [
    ('_1H', '60 MINUTES', '5 MINUTES'),
    ('_6H', '6 HOURS', '1 HOUR'),
    ('_1D', '24 HOURS', '1 HOUR'),
    ('_1W', '7 DAYS', '1 DAY'),
    ('_1M', '30 DAYS', ' 1 DAY')
]

schemas = [
    """
CREATE STREAM {}WEBLOGS_SCHEMA (
    client_request_host VARCHAR,
    client_ip VARCHAR,
    client_url VARCHAR,
    ua_name VARCHAR,
    http_response_code VARCHAR,
    ts_timestamp VARCHAR,
    reply_length_bytes BIGINT,
    geoip STRUCT<country_code2 VARCHAR>,
    cache_result VARCHAR
) WITH (
    kafka_topic = 'deflect.logs',
    partitions = 3,
    value_format = 'json',
    timestamp = 'ts_timestamp',
    timestamp_format = 'dd/LLL/yyyy:HH:mm:ss ZZZ'
);
    """,
    """
CREATE STREAM {}BANJAX_SCHEMA (
    http_host VARCHAR,
    client_ip VARCHAR,
    action VARCHAR,
    uripath VARCHAR,
    user_agent STRUCT<name VARCHAR>,
    geoip STRUCT<country_code2 VARCHAR>,
    datestamp VARCHAR
) WITH (
    kafka_topic = 'banjax',
    partitions = 3,
    value_format = 'json',
    timestamp = 'datestamp',
    timestamp_format = '''[''yyyy-MM-dd''T''HH:mm:ss'']'''
);
    """
]

streams = [
    """
CREATE STREAM {}WEBLOGS_WWW AS
    SELECT
        REGEXP_REPLACE(client_request_host, 'www[\.]', '') as host_no_www,
        client_url,
        ts_timestamp,
        reply_length_bytes,
        geoip->country_code2 as country_code,
        client_ip,
        ua_name,
        http_response_code,
        CASE
            WHEN cache_result <> 'TCP_MISS' THEN 1
         ELSE 0
        END AS cached
    FROM {}WEBLOGS_SCHEMA;
    """,
    """
CREATE STREAM STATS_WEBLOGS 
  WITH (PARTITIONS=3) AS 
  SELECT * 
   FROM STATS_WEBLOGS_WWW
   PARTITION BY host_no_www;    
    """,
    """
CREATE STREAM {}BANJAX_WWW AS
    SELECT
        REGEXP_REPLACE(http_host, 'www[\.]', '') as host_no_www,
        client_ip,
        uripath,
        user_agent->name as ua_name,
        geoip->country_code2 as country_code
    FROM {}BANJAX_SCHEMA;   
    """,
    """
CREATE STREAM STATS_BANJAX 
  WITH (PARTITIONS=3) AS 
  SELECT * 
   FROM STATS_BANJAX_WWW
   PARTITION BY host_no_www;      
    """
]

minimum_queries = [
   """
CREATE TABLE {}WEBLOGS_5M  AS
SELECT host_no_www, EARLIEST_BY_OFFSET(host_no_www) as host,
sum (reply_length_bytes) as allbytes,
sum (cached*reply_length_bytes) as cachedbytes,
count (*) as allhits,
sum(cached) as cachedhits,
COLLECT_LIST (client_ip) as client_ip,
HISTOGRAM (country_code) as country_codes,
HISTOGRAM (client_url) as viewed_pages,
HISTOGRAM (ua_name) as ua,
HISTOGRAM (http_response_code) as http_code,
TIMESTAMPTOSTRING(WINDOWEND, 'yyy-MM-dd HH:mm:ss', 'UTC') as window_end
 FROM {}WEBLOGS
 WINDOW TUMBLING (SIZE 5 MINUTES)
 GROUP BY host_no_www;
   """,
   """
CREATE TABLE {}BANJAX_5M AS
SELECT host_no_www, EARLIEST_BY_OFFSET(host_no_www) as host,
count (*) as bans,
COLLECT_LIST (client_ip) as client_ip,
HISTOGRAM (country_code) as country_codes,
HISTOGRAM (uripath) as target_url,
COUNT_DISTINCT (client_ip) as uniquebots,
TIMESTAMPTOSTRING(WINDOWEND, 'yyy-MM-dd HH:mm:ss', 'UTC') as window_end
 FROM {}BANJAX
 WINDOW TUMBLING (SIZE 5 MINUTES)
 GROUP BY host_no_www;     
 """

]

tumbling_queries = [
    """
CREATE TABLE {}TUMBLING_WEBLOGS{}  AS
SELECT host_no_www, EARLIEST_BY_OFFSET(host_no_www) as host,
sum (reply_length_bytes) as allbytes,
sum (cached*reply_length_bytes) as cachedbytes,
count (*) as allhits,
sum(cached) as cachedhits,
COUNT_DISTINCT (client_ip) as unique_ips,
HISTOGRAM (country_code) as country_codes,
TIMESTAMPTOSTRING(WINDOWEND, 'yyy-MM-dd HH:mm:ss', 'UTC') as window_end
 FROM {}WEBLOGS
 WINDOW TUMBLING (SIZE {})
 GROUP BY host_no_www;
    """,
    """
CREATE TABLE {}TUMBLING_BANJAX{} AS
SELECT host_no_www, EARLIEST_BY_OFFSET(host_no_www) as host,
COUNT_DISTINCT (*) as bans,
HISTOGRAM (country_code) as country_codes,
COUNT_DISTINCT (client_ip) as uniquebots,
TIMESTAMPTOSTRING(WINDOWEND, 'yyy-MM-dd HH:mm:ss', 'UTC') as window_end
 FROM {}BANJAX
 WINDOW TUMBLING (SIZE {})
 GROUP BY host_no_www;    
    """
]

hopping_queries = [
    """
CREATE TABLE {}HOPPING_WEBLOGS{}  AS
SELECT host_no_www, EARLIEST_BY_OFFSET(host_no_www) as host,
sum (reply_length_bytes) as allbytes,
sum (cached*reply_length_bytes) as cachedbytes,
COUNT_DISTINCT (client_ip) as unique_ips,
count (*) as allhits,
sum(cached) as cachedhits,
HISTOGRAM (country_code) as country_codes,
HISTOGRAM (client_url) as viewed_pages,
HISTOGRAM (ua_name) as ua,
HISTOGRAM (http_response_code) as http_code,
TIMESTAMPTOSTRING(WINDOWEND, 'yyy-MM-dd HH:mm:ss', 'UTC') as window_end
 FROM {}WEBLOGS
 WINDOW HOPPING (SIZE {}, ADVANCE BY {})
 GROUP BY host_no_www;
    """
    ,
    """
CREATE TABLE {}HOPPING_BANJAX{} AS
SELECT host_no_www, EARLIEST_BY_OFFSET(host_no_www) as host,
COUNT_DISTINCT (*) as bans,
HISTOGRAM (country_code) as country_codes,
HISTOGRAM (uripath) as target_url,
COUNT_DISTINCT (client_ip) as uniquebots,
TIMESTAMPTOSTRING(WINDOWEND, 'yyy-MM-dd HH:mm:ss', 'UTC') as window_end
 FROM {}BANJAX
 WINDOW HOPPING (SIZE {}, ADVANCE BY {})
 GROUP BY host_no_www;    
    """
]

for q in schemas:
    print(q.format(prefix))

for q in streams:
    print(q.format(prefix, prefix))

for q in minimum_queries:
    print(q.format(prefix, prefix))


# for q in tumbling_queries:
#     for window in tumbling_windows:
#         print(q.format(prefix, window[0], prefix, window[1]))
#
# for q in hopping_queries:
#     for window in hopping_windows:
#         print(q.format(prefix, window[0], prefix, window[1], window[2]))
