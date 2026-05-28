CREATE STREAM STATS_WEBLOGS_SCHEMA (
    client_request_host VARCHAR,
    client_ip VARCHAR,
    client_url VARCHAR,
    client_ua VARCHAR,
    http_response_code VARCHAR,
    datestamp VARCHAR,
    reply_length_bytes BIGINT,
    geoip STRUCT<country_code2 VARCHAR>,
    cache_result VARCHAR,
    content_type VARCHAR
) WITH (
    kafka_topic = 'deflect.log',
    partitions = 3,
    value_format = 'json',
    timestamp = 'datestamp',
    timestamp_format = 'dd/LLL/yyyy:HH:mm:ss ZZZ'
);


CREATE STREAM STATS_BANJAX_SCHEMA (
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


CREATE STREAM STATS_WEBLOGS_WWW AS
    SELECT
        REPLACE(client_request_host, 'www.', '') as host_no_www,
        client_url,
        CASE
         WHEN (http_response_code = '200' or http_response_code = '304')
                and (
                  content_type = 'text/html; charset=utf-8' or
                  content_type = 'text/plain; charset=utf-8' or
                  content_type = 'application/pdf; charset=utf-8' or
                  content_type = 'application/msword; charset=utf-8' or
                  content_type = '-' or
                  content_type = 'text/html; charset=UTF-8' or
                  content_type = 'text/plain; charset=UTF-8' or
                  content_type = 'application/pdf; charset=UTF-8' or
                  content_type = 'application/msword; charset=UTF-8')
         THEN
REGEXP_REPLACE(client_url,'/(robots.txt|xmlrpc.php|10k|.*(jpeg|js|jpg|ico|css|json|png|gif|class|bmp|rss|xml|swf))', '')
         ELSE
            ''
        END as client_url_filtered,

        datestamp,
        reply_length_bytes,
        geoip->country_code2 as country_code,
        client_ip,
        client_ua,
        http_response_code,
        CASE
            WHEN
            cache_result = 'HIT' or
            cache_result = 'STALE' or
            cache_result = 'UPDATING' or
            cache_result = 'REVALIDATED'
            THEN 1
         ELSE 0
        END AS cached
    FROM STATS_WEBLOGS_SCHEMA;


CREATE STREAM STATS_WEBLOGS
  WITH (PARTITIONS=3) AS
  SELECT *
   FROM STATS_WEBLOGS_WWW
   PARTITION BY host_no_www;


CREATE STREAM STATS_BANJAX_WWW AS
    SELECT
        REPLACE(http_host, 'www.', '') as host_no_www,
        client_ip,
        CASE WHEN uripath IS null THEN ' ' ELSE uripath END as uripath,
        user_agent->name as ua_name,
        geoip->country_code2 as country_code
    FROM STATS_BANJAX_SCHEMA
    WHERE action = 'banned';


CREATE STREAM STATS_BANJAX_PARTITIONED
  WITH (PARTITIONS=3) AS
  SELECT *
   FROM STATS_BANJAX_WWW
   PARTITION BY host_no_www;


CREATE TABLE STATS_BANJAX_UNIQUE_TABLE AS
  SELECT
  host_no_www,
  country_code,
  client_ip,
  uripath,
  EARLIEST_BY_OFFSET(host_no_www) AS host2,
  EARLIEST_BY_OFFSET(client_ip) as client_ip2,
  EARLIEST_BY_OFFSET(country_code) as country_code2,
  EARLIEST_BY_OFFSET(uripath) as uripath2,
  COUNT(client_ip) as ip_count,
  TIMESTAMPTOSTRING(WINDOWEND, 'yyy-MM-dd HH:mm:ss', 'UTC') as window_end
   FROM STATS_BANJAX_PARTITIONED
   WINDOW TUMBLING (SIZE 5 MINUTES)
   GROUP BY host_no_www, country_code, client_ip, uripath;


    CREATE STREAM STATS_BANJAX_UNIQUE_SCHEMA
     (
    host2 VARCHAR,
    client_ip2 VARCHAR,
    country_code2 VARCHAR,
    uripath2 VARCHAR,
    ip_count INTEGER
) WITH (
    kafka_topic = 'STATS_BANJAX_UNIQUE_TABLE',
    partitions = 3,
    value_format = 'json'
);


    CREATE STREAM STATS_BANJAX_UNIQUE AS
    SELECT
        host2,
        client_ip2,
        country_code2,
        uripath2
    FROM STATS_BANJAX_UNIQUE_SCHEMA
    WHERE IP_COUNT = 1
    PARTITION BY host2;


 CREATE TABLE STATS_WEBLOGS_5M  AS
 SELECT host_no_www, EARLIEST_BY_OFFSET(host_no_www) as host,
 sum (reply_length_bytes) as allbytes,
 sum (cached*reply_length_bytes) as cachedbytes,
 count (*) as allhits,
 sum(cached) as cachedhits,
 COLLECT_SET (client_ip) as client_ip,
 HISTOGRAM (country_code) as country_codes,
 HISTOGRAM (client_url) as client_url,
 HISTOGRAM (client_url_filtered) as viewed_pages,
 COUNT(client_url_filtered) as viewed_page_count,
 HISTOGRAM (client_ua) as ua,
 HISTOGRAM (http_response_code) as http_code,
 TIMESTAMPTOSTRING(WINDOWEND, 'yyy-MM-dd HH:mm:ss', 'UTC') as window_end
  FROM STATS_WEBLOGS
  WINDOW TUMBLING (SIZE 5 MINUTES)
  GROUP BY host_no_www;


 CREATE TABLE STATS_BANJAX_5M AS
 SELECT host2, EARLIEST_BY_OFFSET(host2) as host,
 COLLECT_SET (client_ip2) as client_ip,
 HISTOGRAM (country_code2) as country_codes,
 HISTOGRAM (uripath2) as target_url,
 COUNT_DISTINCT (client_ip2) as uniquebots,
 TIMESTAMPTOSTRING(WINDOWEND, 'yyy-MM-dd HH:mm:ss', 'UTC') as window_end
  FROM STATS_BANJAX_UNIQUE
  WINDOW TUMBLING (SIZE 5 MINUTES)
  GROUP BY host2;