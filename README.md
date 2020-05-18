## Contents

- [What is Baskerville](#what-is-baskerville)
    - [Overview](#overview)
    - [Technology](#technology)  
- [Requirements](#requirements)
- [Useful Definitions](#useful-definitions)
    - [Runtime](#runtime)    
    - [Model](#model)    
    - [Anomaly detector](#anomaly-detector)    
    - [Request set](#request-set)    
    - [Subset](#subset)    
    - [Time bucket](#time-bucket)
- [Baskerville Engine](#baskerville-engine)
  - [Pipelines](#pipelines):
    - [Raw Logs](#raw-logs)
    - [Elastic Search](#elastic-search)
    - [Kafka](#kafka)
    - [Training](#training)
  - [Predictions and ML](#predictions-and-ml)
- [Installation](#installation)
- [Configuration](#configuration)
- [How to run](#how-to-run)
- [Testing](#testing)
- [Docs](#docs)
- [TODO](#todo)
- [Contributing](#contributing)
  - [Contributors](#contributors)

## What is Baskerville

Manual identification and mitigation of (DDoS) attacks on websites is a difficult and time-consuming task with many challenges. This is why Baskerville was created, to identify the attacks directed to Deflect protected 
websites as they happen and give the infrastructure the time to respond properly. Baskerville is an analytics engine that leverages Machine Learning to distinguish between normal and abnormal web traffic behavior.

The challenges:
- Be fast enough  to make it count
- Be able to  adapt to traffic
 (Apache Spark, Apache Kafka)
- Provide actionable info (A prediction and a score for an IP)
- Provide reliable predictions (Probation period & feedback)
- As with any ML project: not enough labelled data (Using normal-ish data - the anomaly/ novelty detector can accept a small percentage of anomalous data in the dataset)


## Overview

Baskerville is the component of the Deflect analysis engine that is used to
decide whether IPs connecting to Deflect hosts are authentic normal
connections, or malicious bots. In order to make this assessment, Baskerville
groups incoming requests into *request sets* by requested host and requesting IP.

For each request set, a selection of *features* are
computed. These are properties of the requests within the request set (e.g.
average path depth, number of unique queries, HTML to image ratio...) that are
intended to help differentiate normal request sets from bot request sets. A supervised
*novelty detector*, trained offline on the feature vectors of a set of normal
request sets, is used to predict whether new request sets are normal or suspicious. The request sets,
their features, trained models, and details of suspected attacks and attributes,
are all saved to a relational database (Postgres currently).

Put simply, the Baskerville *engine* is the workhorse for consuming,
processing, and saving the output from input web logs. This engine can be run as
Baskerville *on-line*, which enables the immediate identification of suspicious IPs, or as Baskerville *off-line*, which conducts this same
analysis for log files saved locally or in an elasticsearch database.

![Deflect and Baskerville](data/img/deflect_and_baskerville.png?raw=true "Deflect and Baskerville")


1. The first step is to get the data (Combined log format) and transport it to the processing system
2. Once we get the logs, the processing system will start transforming the input to feature vectors and subsequently predictions for target-ip pairs
3. The information that comes out from the processing is stored in a database
4. At the same time, while the system is running, there are other systems in place to monitor its process and alert if anything is out of place
5. And eventually, the insight we gained from the process will return to the source so that actions can be taken

## Technology
- [Apache Spark](https://spark.apache.org)
- [Apache Kafka](https://kafka.apache.org)
- [Postgres (with the Timescale extension)](https://github.com/timescale/timescaledb)
- Metrics:
  - [Prometheus (with the Postgres adapter for storage)](https://prometheus.io)
  - [Grafana](https://grafana.com)

For the Machine Learning:
- [Pyspark ML](https://spark.apache.org/docs/latest/api/python/pyspark.ml.html)
- [Isolation Forest](https://github.com/titicaca/spark-iforest) (a slightly modified version)
- [Scikit Learn](https://scikit-learn.org/stable/) (not actively used because of performance issues)

## Requirements
- Python >= 3.6 (ideally 3.6 because there have been some issues with 3.7 testing and numpy in the past)
- Postgres 10
- If you want to use the IForest model, then https://github.com/titicaca/spark-iforest is required (`pyspark_iforest.ml`)
- Java 8 needs to be in place (and in PATH) for [Spark](https://spark.apache.org/docs/2.4.5/)
(Pyspark version >=2.4.4) to work,
- The required packages in `requirements.txt`
- Tests need `pytest`, `mock` and `spark-testing-base`
- For the Elastic Search pipeline: access to the [esretriever](https://github.com/equalitie/esretriever.git)
repository 
- Optionally: access to the [Deflect analytics ecosystem](https://github.com/equalitie/deflect-analytics-ecosystem)
repository (to run Baskerville dockerized components like Postgres, Kafka, Prometheus, Grafana etc).

## Installation
For Baskerville to be fully functional we need to install the following:
```bash
# clone and install spark-iforest
git clone https://github.com/titicaca/spark-iforest
cd spark-iforest/python
pip install .

# clone and install esretriever - for the ElasticSearch pipeline
cd ../../
git clone https://github.com/equalitie/esretriever.git
cd esretriever
pip install .

# Finally, clone and install baskerville
cd ../
git clone https://github.com/equalitie/baskerville.git
cd baskerville
pip install .[test]
```

Note: In Windows you might have to install `pypandoc` first for pyspark to install.

## Configuration
Since it is good practice to use environment variables for the configuration, the following can be set and used as follows:

```bash
cd baskerville
# must be set:
export BASKERVILLE_ROOT=$(pwd)  # full path to baskerville folder
export DB_HOST=the db host (docker ip if local)
export DB_PORT=5432
export DB_USER=postgres user
export DB_PASSWORD=secret

# optional - pipeline dependent
export ELK_USER=elasticsearch
export ELK_PASSWORD=changeme
export ELK_HOST=the elasticsearch host:port (localhost:9200)
export KAFKA_HOST=kafka host:port (localhost:9092)
```

A basic configuration for running the raw log pipeline is the following - rename [`baskerville/conf/conf_rawlog_example_baskerville.yaml`](conf/conf_rawlog_example_baskerville.yaml) to `baskerville/conf/baskerville.yaml`:
```yaml
---
database:
  name: baskerville                   # the database name
  user: !ENV ${DB_USER}
  password: !ENV '${DB_PASS}'
  type: 'postgres'
  host: !ENV ${DB_HOST}
  port: !ENV ${DB_PORT}
  maintenance:                        # Optional, for data partitioning and archiving
    partition_table: 'request_sets'   # default value
    partition_by: week                # partition by week or month, default value is week
    partition_field: created_at       # which field to use for the partitioning, this is the default value, can be omitted
    strict: False                     # if False, then for the week partition the start and end date will be changed to the start and end of the respective weeks. If true, then the dates will remain unchanged. Be careful to be consistent with this.
    data_partition:                   # Optional: Define the period to create partitions for
      since: 2020-01-01               # when to start partitioning
      until: "2020-12-31 23:59:59"    # when to stop partitioning

engine:
  storage_path: !ENV '${BASKERVILLE_ROOT}/data'
  raw_log:
    paths:                    # optional, a list of logs to parse - they will be parsed subsequently
      - !ENV '${BASKERVILLE_ROOT}/data/samples/test_data_1k.json'  # sample data to run baskerville raw log pipeline with
  cache_expire_time: 604800       # sec (604800 = 1 week)
  model_id: -1                    # optional, -1 returns the latest model in the database
  extra_features:                 # useful when we need to calculate more features than the model requests or when there is no model
      - css_to_html_ratio         # feature names have the following convention: class FeatureCssToHtmlRatio --> 'css_to_html_ratio'
      - image_to_html_ratio
      - js_to_html_ratio
      - minutes_total
      - path_depth_average
      - path_depth_variance
      - payload_size_average
  data_config:
    schema: !ENV '${BASKERVILLE_ROOT}/data/samples/sample_log_schema.json'
  logpath: !ENV '${BASKERVILLE_ROOT}/baskerville.log'
  log_level: 'ERROR'

spark:
  app_name: 'Baskerville'   # the application name - can be changed for two different runs - used by the spark UI
  master: 'local'           # the ip:port of the master node, e.g. spark://someip:7077 to submit to a cluster
  parallelism: -1           # controls the number of tasks, -1 means use all cores - used for local master
  log_level: 'INFO'         # spark logs level
  storage_level: 'OFF_HEAP' # which strategy to use for storing dfs - valid values are the ones found here: https://spark.apache.org/docs/2.4.0/api/python/_modules/pyspark/storagelevel.html default: OFF_HEAP
  jars: !ENV '${BASKERVILLE_ROOT}/data/jars/postgresql-42.2.4.jar,${BASKERVILLE_ROOT}/data/spark-iforest-2.4.0.jar' # or /path/to/jars/mysql-connector-java-8.0.11.jar
  spark_driver_memory: '6G' # depends on your dataset and the available ram you have. If running locally 6 - 8 GB should be a good choice, depending on the amount of data you need to process
  metrics_conf: !ENV '${BASKERVILLE_ROOT}/data/spark.metrics'  # Optional: required only  to export spark metrics
  jar_packages: 'com.banzaicloud:spark-metrics_2.11:2.3-2.0.4,io.prometheus:simpleclient:0.3.0,io.prometheus:simpleclient_dropwizard:0.3.0,io.prometheus:simpleclient_pushgateway:0.3.0,io.dropwizard.metrics:metrics-core:3.1.2'  # required to export spark metrics
  jar_repositories: 'https://raw.github.com/banzaicloud/spark-metrics/master/maven-repo/releases' # Optional: Required only to export spark metrics
  event_log: True
  serializer: 'org.apache.spark.serializer.KryoSerializer'
```

In [`baskerville/conf/conf_example_baskerville.yaml`](conf/conf_example_baskerville.yaml) you can see all the possible configuration options.

Example of configurations for the other pipelines:
- kafka: [`baskerville/conf/conf_kafka_example_baskerville.yaml`](conf/conf_kafka_example_baskerville.yaml)
- es: [`baskerville/conf/conf_es_example_baskerville.yaml`](conf/conf_es_example_baskerville.yaml)
- training: [`baskerville/conf/conf_training_example_baskerville.yaml`](conf/conf_training_example_baskerville.yaml)

## How to run
In general, there are two ways to run Baskerville, a pure python one or using `spark-submit`, both are detailed below.

The full set of options:
```bash
usage: main.py [-h] [-s] [-e] [-t] [-c CONF_FILE] pipeline

positional arguments:
  pipeline              Pipeline to use: es, rawlog, or kafka

optional arguments:
  -h, --help            show this help message and exit
  -s, --simulate        Simulate real-time run using kafka
  -e, --startexporter   Start the Baskerville Prometheus exporter at the
                        specified in the configuration port
  -t, --testmodel       Add a test model in the models table
  -c CONF_FILE, --conf CONF_FILE
                        Path to config file
```

- For the python approach (which I find it easier when running locally):
```bash
cd baskerville/src/baskerville
python3 main.py [which pipeline to use] [should baskerville register and export metrics? if yes use the -e flag]
# which means:
python3 main.py [kafka||rawlog||es] [-e] [-t] [-s]
```

Note: you can replace `python3 main.py` with `baskerville` like this:
```python
baskerville [kafka||rawlog||es] [-e] [-t] [-s]
```

- And for [`spark-submit`](https://spark.apache.org/docs/latest/submitting-applications.html) (which is usually used when you want to submit a Baskerville Application to a cluster):

```bash
cd baskerville
export BASKERVILLE_ROOT=$(pwd)
spark-submit --jars $BASKERVILLE_ROOT/data/jars/[relevant jars like postgresql-42.2.4.jar,spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar etc] --conf [spark configurations if any - note that these will override the baskerville.yaml configurations] $BASKERVILLE_ROOT/src/baskerville/main.py [kafka||rawlog||es] [-e] -c $BASKERVILLE_ROOT/conf/baskerville.yaml (or the absolute path to your configuration)
```

Examples:
```bash
cd baskerville
export BASKERVILLE_ROOT = $(pwd) # or full path to baskerville
# minimal spark-submit for the raw logs pipeline:
spark-submit --jars $BASKERVILLE_ROOT/data/jars/postgresql-42.2.4.jar --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=2g $BASKERVILLE_ROOT/src/baskerville/main.py rawlog -c $BASKERVILLE_ROOT/conf/baskerville.yaml 
# or
spark-submit --packages org.postgresql:postgresql:42.2.4 --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=2g $BASKERVILLE_ROOT/src/baskerville/main.py rawlog -c $BASKERVILLE_ROOT/conf/baskerville.yaml 

# minimal spark-submit for the elastic search pipeline:
spark-submit --jars $BASKERVILLE_ROOT/data/jars/postgresql-42.2.4.jar,$BASKERVILLE_ROOT/data/jars/elasticsearch-spark-20_2.11-5.6.5.jar --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=2g $BASKERVILLE_ROOT/src/baskerville/main.py es -c $BASKERVILLE_ROOT/conf/baskerville.yaml 

# minimal spark-submit for the kafka pipeline:
spark-submit --jars $BASKERVILLE_ROOT/data/jars/postgresql-42.2.4.jar,$BASKERVILLE_ROOT/data/jars/spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar --conf spark.memory.offHeap.enabled=true --conf spark.memory.offHeap.size=2g $BASKERVILLE_ROOT/src/baskerville/main.py kafka -c $BASKERVILLE_ROOT/conf/baskerville.yaml

# note the spark.memory.offHeap.size does not have to be 2G and it depends on the size of your data
```
The paths in spark-submit must be absolute and accessible from all the workers.

Note for Windows:
In Winows Spark might not initialize. If so, set $HADOOP_HOME as follows and download the appropriate `winutils.exe` from [https://github.com/steveloughran/winutils](https://github.com/steveloughran/winutils)

```bash
mkdir c:\hadoop\bin
export HADOOP_HOME=c:\hadoop
cp $HOME\Downloads\winutils.exe $HADOOP_HOME\bin
```
##### Necessary services/ components for each pipeline:
``
For Baskerville `kafka` you'll need:
  - Kafka
  - Zookeeper
  - Postgres
  - Prometheus  [optional]
  - Grafana     [optional]

For Baskerville `rawlog` you'll need:
  - Postgres
  - Elastic Search
  - Prometheus  [optional]
  - Grafana     [optional]

For Baskerville `es` you'll need:
  - Postgres
  - Elastic Search
  - Prometheus  [optional]
  - Grafana     [optional]

__An ElasticSearch service is not provided.__

For Baskerville `training` you'll need:
  - Postgres
  - Prometheus  [optional]
  - Grafana     [optional]``

#### Using Docker - for the dev environment
Under `baskerville/container` there is a Dockerfile that sets up the appropriate version of Java, Python and sets up Baskerville.
To run this, run `docker-compose up` in the directory where the `docker-compose.yaml` is. 

Note that you will have to provide the relevant environment variables as defined in the docker-compose file under `args`.
An easy way to do this is to use a `.env` file. Rename the `dot_enf_file` to `.env` and modify it accordingly:
```yaml
# must be set:
BASKERVILLE_BRANCH=e.g. master, develop etc, the branch to pull from
DB_HOST=the db host (docker ip if local)
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=secret

# optional - pipeline dependent
ELK_USER=elasticsearch
ELK_PASSWORD=changeme
ELK_HOST=the elasticsearch host (docker ip if local):port (default port is 9200)
KAFKA_HOST=kafka ip (docker ip if local):port (default:9092)
```
The docker ip can be retrieved by `$(ipconfig getifaddr en0)`, assuming `en0` is you active network interface.
The `${DOCKER_IP}` is used when the systems are running on your local docker. Otherwise, just use the appropriate environment variable.
E.g. `- ELK_HOST=${ELK_HOST}`, where `${ELK_HOST}` is set in `.env` to the ip of the elasticsearch instance.

The command part is how you want baskerville to run. E.g. for the rawlog pipeline - with metrics exporter:
```yaml
command: python ./main.py -c /app/baskerville/conf/baskerville.yaml rawlog -e
```
Once you have test data in Postgres, you can train:
```yaml
command: python ./main.py -c /app/baskerville/conf/baskerville.yaml training
```
Or use the provided test model:
```yaml
command: python ./main.py -c /app/baskerville/conf/baskerville.yaml rawlog -e -t
```
And so on. See [Running](#running) section for more options.

Make sure you have a Postgres instance running. You can use [the deflect-analytics-ecosystem's docker-compose to spin up a postgres instance](https://github.com/equalitie/deflect-analytics-ecosystem/blob/master/docker-compose.yaml#L15) 


In [deflect-analytics-ecosystem](https://github.com/equalitie/deflect-analytics-ecosystem) you can find a docker-compose that creates all the aforementioned components and runs Baskerville too. **It should be the simplest way to test Baskerville out**. To launch the relevant services, comment out what you don't need from the [`docker-compose.yaml`](https://github.com/equalitie/deflect-analytics-ecosystem/blob/master/docker-compose.yaml) and run

```bash
docker-compose up
```
in the directory with the `docker-compose.yaml` file.

*Note*: For the Kafka service, before you run `docker-compose up` set:

```bash
export DOCKER_KAFKA_HOST=currentl-local-ip or $(ipconfig getifaddr en0) # where en0 is your current active interface
```

##### How to check it worked as it should?
The simplest way is to check the database.
```sql
-- Give baskerville is the database name defined in thec configuration:
SELECT count(id) from request_sets where id_runtime in (select max(id) from runtimes); -- if you used the test data, it should be 1000 after a full successful execution
SELECT * FROM baskerville.request_sets;  -- fields should be complete, e.g. features should be something like the following
```

Request_set features example:
```json
{
    "top_page_to_request_ratio": 1,
    "response4xx_total": 0,
    "unique_path_total": 2,
    "request_interval_variance": 0,
    "unique_path_to_request_ratio": 1,
    "image_total": 2,
    "css_total": 0,
    "js_total": 0,
    "css_to_html_ratio": 0,
    "unique_query_to_unique_path_ratio": 0,
    "unique_ua_rate": 1,
    "top_page_total": 2,
    "js_to_html_ratio": 0,
    "request_total": 2,
    "image_to_html_ratio": 200,
    "request_interval_average": 0,
    "unique_path_rate": 1,
    "minutes_total": 0,
    "unique_query_rate": 0,
    "html_total": 0,
    "path_depth_variance": 50,
    "path_depth_average": 5,
    "unique_query_total": 0,
    "unique_ua_total": 2,
    "payload_size_average": 10.40999984741211,
    "response4xx_to_request_ratio": 0,
    "payload_size_log_average": 9.250617980957031
}

```
## Metrics
Grafana is a metrics visualization web application that can be configured to
display several dashboards with charts, raise alerts when metric crosses a user defined threshold and notify through mail or other means. Within Baskerville, under data/metrics, there is an importable to Grafana dashboard which presents the statistics of the Baskerville engine in a customisable manner. It is intended to be the principal visualisation and alerting tool of incoming Deflect traffic, displaying metrics in graphical form.
Prometheus is the metric storage and aggregator that provides Grafana with the charts data.
Besides the spark ui - which usually is under `http://localhost:4040`, there is a set of metrics for the Baskerville engine set up, using the Python Prometheus library. To see those metrics,
include the `-e` flag and go to the configured localhost port (`http:// localhost:8998/metrics` by default). You will need a configured Prometheus instance and a Grafana instance to be able to
visualize them using the auto generated [baskerville dashboard](data/metrics/Baskerville-metrics-dashboard.json), which is saved in the data directory, and can
be imported in Grafana.
There is also an [Anomalies Dashboard](/data/metrics/anomalies-dashboard.json) and a [Kafka Dashboard](/data/metrics/kafka-dashboard.json) under `data/metrics`.

![Baskerville's Anomalies Dashboard](data/img/anomalies_dashboard.png?raw=true "Baskerville's Anomalies Dashboard")

![Kafka Dashboard](data/img/kafka_dashboard.png?raw=true "Baskerville's Kafka Dashboard")

To view the spark generated metrics, you have to include the configuration for the jar packages:
```yaml
  metrics_conf: !ENV '${BASKERVILLE_ROOT}/data/metrics/spark.metrics'
  jar_packages: 'io.prometheus:simpleclient:0.3.0,io.prometheus:simpleclient_dropwizard:0.3.0,io.prometheus:simpleclient_pushgateway:0.3.0,io.dropwizard.metrics:metrics-core:3.1.2,com.banzaicloud:spark-metrics_2.11:2.3-2.0.4'
  jars_repositories: 'https://raw.github.com/banzaicloud/spark-metrics/master/maven-repo/releases'
```

And also have a prometheus push gateway set up. You can use [the deflect-analytics-ecosystem's docker-compose for that](https://github.com/equalitie/deflect-analytics-ecosystem/blob/master/docker-compose.yaml#L44). 

## Testing

__Unit Tests__:
Basic unittests for the features and the pipelines have been implemented.
To run them:
```
python3 -m pytest tests/
```

Note: Many of the tests have to use a spark session, so unfortunately it will take some time for all to complete. (~=2 minutes currently)

__Functional Tests__:
No functional tests exist yet. They will be added as the structure stabilizes.

## Docs
We use `pydoc` to generate docs for Baskerville under `baskerville/docs` folder. 
```shell script
# use --force to overwrite docs
 pdoc --html --force --output-dir docs baskerville
```
Then open (`docs/baskerville/index.html`)[docs/baskerville/index.html] with a browser.

### Useful Definitions

- **Runtime**: The time and details for each time Baskerville runs, from start to finish of a Pipeline. E.g. start time, number of subsets processed etc.

- **Model**: The Machine Learning details that are stored in the database.

- **Anomaly detector**:
The structure that holds the ML related steps, like the algoithm, the respective scaler etc. Current algorithm is Isolation Forest (scikit or pyspark implementation). OneClassSvm was a candidate but it was slow and the results less accurate.

- **Request set**:
A request set is the behaviour (the characteristics of the requests being made) of a specific IP towards a specific target for a certain amount of time (runtime).

- **Subset**:
Given that a request set has a duration of a runtime, then the subset is the behaviour of the IP-to-target for a specific time-window (`time_bucket`)

- **Time bucket**:
It is used to define how often should Baskerville consume and process logs. The default value is `120` (seconds).

### Baskerville Engine
The main Baskerville engine consumes web logs and uses them to compute
request sets (i.e. the groups of requests made by each IP-host pair) and extract the request set features. It applies a trained novelty detection algorithm to predict whether each request set is normal or anomalous within the current time window. It saves the request set
features and predictions to the Baskerville storage database. It can also cross-references
incoming IP addresses with attacks logged in the database, to determine if a
label (known malicious or known benign) can be applied to the request set.

![Baskerville's Batch Processing Flow](data/img/batch_processing_flow.png?raw=true "Baskerville's Batch Processing Flow")


Each request-set is divided into *subsets*.
Subsets have a two-minute length (configurable), and the request set
features (and prediction) are updated at the end of each subset using a feature-specific update method (discussed
[here](data/feature_overview/feature_overview.md)).

For nonlinear features, the feature value will be dependent on the subset
length, so for this reason, logs are processed in two-minute subsets even when
not being consumed live. This is also discussed in depth in the feature document above.

![Baskerville's Time Window Processing Flow](data/img/tw_processing.png?raw=true "Baskerville's Time Window Processing Flow")


The Baskerville engine utilises [Apache Spark](https://spark.apache.org/); an
analytics framework designed for the purpose of large-scale data processing. The decision to use Spark in Baskerville was made to ensure that the engine can
achieve a high enough level of efficiency to consume and process web logs in real time, and thus run continuously as part of the Deflect ecosystem.

![Baskerville's Basic Flow](data/img/basic_flow.png?raw=true "Baskerville's Basic Flow")

#### Pipelines
There are four main pipelines. Three have to do with the different kinds of input and the fourth one with the ML.
Baskerville is designed to consume web logs from various sources in predefined intervals (`time bucket` set to 120 seconds by default)

##### Raw logs
Read from json logs, process, store in postgres.
![Baskerville's Raw logs Processing Flow](data/img/raw_logs_pipeline.png?raw=true "Baskerville's Raw logs Processing Flow")

##### Elastic Search
Read from Elastc Search splitting the period in smaller periods so as not to overload the ELK cluster, process, store in postgres. (optionally store logs locally too)
![Baskerville's Elastic Search Processing Flow](data/img/elastic_search_pipeline.png?raw=true "Baskerville's Elastic Search Processing Flow")

##### Kafka
Read from a topic every `time_bucket` seconds, process, store in postgres.
![Baskerville's Kafka Processing Flow](data/img/kafka_pipeline.png?raw=true "Baskerville's Kafka Processing Flow")

##### Training
The pipeline used to train the machine learning model. Reads preprocessed data from Postgres, trains, saves model.

Prediction is optional in the first 3 pipelines - which means you may run the pipelines only to process the data, e.g. historic data, and then use the training pipeline to train/ update the model.

#### Predictions and ML
Because of the lack of a carefully curated labelled dataset, the approach being used here is to have an anomaly / novelty detector, like OneClassSVM or Isolation Forest trained on **mostly normal** traffic. We can train on data from days we know there have been no major incidents and still be accurate enough. More details here: [Deflect Labs Report #5](https://equalit.ie/deflect-labs-report-5-baskerville/)
The output of the prediction process is the prediction and an anomaly score to help indicate confidence in prediction. The output accompanies a specific request set, an IP-target pair for a specific (`time_bucket`) window.

Note: Isolation Forest was preferred to OneClassSVM because of speed and better accuracy.

##### Features
Under [`src/baskerville/features`](src/baskerville/features) you can find all the currently implemented features, like:
- Css to html ratio
- Image to html ratio
- Js to html ratio
- Minutes total
- Path depth average
- Path depth variance
- Payload size average
- Payload size log average
- Request interval average
- Request interval variance
- Request total
- Response 4xx to request ratio
- Top page to request ratio
- Unique path rate
- Unique path to request ratio
- Unique query rate
- Unique query to unique path ratio

and many more.

Most of the features are `updateable`, wich means, they **take the past into consideration**. For this purpose, we keep a **request set cache** for a predefined amount of time (1 week by default), where we store the details and feature vectors for previous request sets, in order to be used in the updating process. This cache is a two layer cache, one layer has all the unique request sets (unique ip-host pairs) for the week and the other one has only the unique ip-host pairs and their respective details for the current time window. More on feature updating [here](data/feature_overview/feature_overview.md).

![Baskerville's Request Set Cache](data/img/request_set_cache.png?raw=true "Baskerville's Request Set Cache")

## Related Projects
- ES Retriever: https://github.com/equalitie/esretriever: A spark wrapper to retrieve data from ElastiSearch
- Deflect Analysis Ecosystem: https://github.com/equalitie/deflect-analysis-ecosystem:
    Docker files for all the components baskerville might need.

## TODO
- Implement full suite of unit and entity tests.
- Conduct model tuning and feature selection / engineering.

## Contributing
If there is an issue or a feature suggestion, please, use the [issue list](https://github.com/equalitie/baskerville/issues) with the appropriate labels. 
The standard process is to create the issue, get feedback for it wherever necessary, create a branch named `issue_issuenumber_short_description`, 
implement the solution for the issue there, along with the relevant tests and then create an MR, so that it can be code reviewed and merged.

### Contributors
- [Anna](https://github.com/apfitzmaurice)
- [Anton](https://github.com/mazhurin)
- [Maria](https://github.com/mkaranasou)
- [Te-k](https://github.com/Te-k)
