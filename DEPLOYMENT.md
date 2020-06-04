[comment]: # Copyright (c) 2020, eQualit.ie inc.
[comment]: # All rights reserved.

[comment]: # This source code is licensed under the BSD-style license found in the 
[comment]: # LICENSE file in the root directory of this source tree.


# Baskerville Deployment

## Common requirements for all servers
* change hostname deflect-baskerville-server1 (2 and 3) 
```
hostnamectl set-hostname deflect-baskerville-01 (02 or 03)
```
* install [git](https://linuxize.com/post/how-to-install-git-on-debian-9/)
```
sudo apt update
sudo apt install git
```
* install [docker](https://docs.docker.com/install/linux/docker-ce/debian/)
```
sudo apt-get update
sudo apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg2 \
    software-properties-common
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add -
sudo apt-key fingerprint 0EBFCD88
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io
sudo usermod -aG docker $USER
```
* install [docker-compose](https://docs.docker.com/compose/install/)
```
sudo curl -L "https://github.com/docker/compose/releases/download/1.25.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
docker-compose --version
```

## Server 3. Postgres, Graphana, Prometheus
* clone Baskerville ecosystem from [github](https://github.com/equalitie/deflect-analytics-ecosystem)
```
git clone https://github.com/equalitie/deflect-analytics-ecosystem.git
cd deflect-analytics-ecosystem
```
* goto Baskerville ecosystem folder `cd deflect-analytics-ecosystem`
* generate a new strong password and replace the default 'secret' postgres password everywhere in `docker-compose.yaml`
* replace graphana password with the new one in `./containers/graphana/Dockerfile `
* add the line `SERVER_3_IP_VALUE prometheus-postgresql-adapter` to /etc/hosts
* add kafka and baskerville jobs to prometheus
```
scrape_configs:
  - job_name: kafka
    static_configs:
      - targets: 
        -'server_2_ip_address:7071'
  - job_name: 'baskerville_exporter'
    static_configs:
      - targets:
        - 'server_1_ip_address:8998'
```
* run `sudo docker-compose up -d`
* run
```
docker run -ti --user root --volume graphana-storage --entrypoint bash grafana/grafana:6.6.0
# in the container:
chown -R root:root /etc/grafana && \
  chmod -R a+r /etc/grafana && \
  chown -R grafana:grafana /var/lib/grafana && \
  chown -R grafana:grafana /usr/share/grafana
```
* make sure postgres is up and running on port 5432 and you can connect from psql or any other client using username `postgres` and the new password
* make sure grafana is up and running on port 3000 and you can login using username `admin` and the new password 

## Server 2. Kafka.
* install dockerized single node Kafka cluster [...](https://docs.confluent.io/current/quickstart/ce-docker-quickstart.html)
```
cd ~/kafka
git clone https://github.com/confluentinc/examples
cd examples
git checkout 5.4.0-post
cp -r cp-all-in-one ../baskerville_kafka
cd ../baskerville_kafka
mkdir broker
```
* add prometheus exporter to the broker container `vi ./broker/Dockerfile`
```
FROM confluentinc/cp-server:5.4.0
ADD prom-jmx-agent-config.yml /usr/app/prom-jmx-agent-config.yml
ADD https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/0.11.0/jmx_prometheus_javaagent-0.11.0.jar /usr/app/jmx_prometheus_javaagent.jar
```
* copy prometheus exporter config from Baskerville repository `baskerville/data/metrics/prom-jmx-agent-config.yaml` to `./broker`
* build and run kafka
```
docker-compose build
docker-compose up -d 
```
* make sure kafka cluster is up and running on port 9092 by createing a console producer/cosumer [Example...](https://kafka.apache.org/quickstart)
* clone kafka security tools[...](https://github.com/confluentinc/confluent-platform-security-tools)
```
cd ~/kafka
git clone https://github.com/confluentinc/confluent-platform-security-tools
cd /confluent-platform-security-tools
```
* generate CA and one broker keystore(agree to everything and specify the same new password)
```
./kafka-generate-ssl.sh
mkdir ../baskerville_kafka/secrets
cp ./truststore/* ../baskerville_kafka/secrets
cp ./keystore/* ../baskerville_kafka/secrets
echo THE_PASSWORD >../baskerville_kafka/secrets/creds
```
* configure SSL `~/kafka/baskerville_kafka/docker-compose.yaml` [...](https://docs.confluent.io/4.0.0/installation/docker/docs/tutorials/clustered-deployment-ssl.html)

```
broker:
  ports:
    - "9092:9092"
  environment:
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,SSL:SSL
    KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,SSL://kafka_server_ip:9092
    KAFKA_SSL_KEYSTORE_FILENAME: kafka.keystore.jks
    KAFKA_SSL_KEYSTORE_CREDENTIALS: creds
    KAFKA_SSL_KEY_CREDENTIALS: creds
    KAFKA_SSL_TRUSTSTORE_FILENAME: kafka.truststore.jks
    KAFKA_SSL_TRUSTSTORE_CREDENTIALS: creds
  volumes:
     - /home/spark/kafka/baskerville_kafka/secrets/:/etc/kafka/secrets/

```
* create a new key for clent connections. Answer no to the first question(you do not want to recreate the truststore). Create a new client password.
```
cd ~/kafka/confluent-platform-security-tools
rm -r keystore
./kafka-generate-ssl.sh
mv ./keystore/kafka.keystore.jks ./keystore/kafka.client.keystore.jks
```
* copy the client key and the trust store to you local destination
```
cd kafka
scp spark@kafka_server_ip:kafka/confluent-platform-security-tools/keystore/kafka.client.keystore.jks .
scp spark@kafka_server_ip:kafka/baskerville_kafka/secrets/kafka.client.truststore.jks .
```
* create ssl configration file
```
echo "bootstrap.servers=kafka_server_ip:9092
security.protocol=SSL
ssl.endpoint.identification.algorithm=
ssl.truststore.location=kafka.truststore.jks
ssl.truststore.password=password_truststore
ssl.keystore.location=kafka.client.keystore.jks
ssl.keystore.password=password_client
ssl.key.password=password_client" >> client-ssl.properties
```
* test kafka producer with SSL :
```
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list kafka_server_ip:9092 --topic anton --producer.config client-ssl.properties
```
* test kafka consumer sith SSL 
```
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server kafka_server_ip:9092 --topic anton --consumer.config client-ssl.properties 
```
* extract the keys in PEM format for use in python:
```
cd kafka
keytool -exportcert -alias localhost -keystore kafka.client.keystore.jks -rfc -file certificate.pem
keytool -v -importkeystore -srckeystore kafka.client.keystore.jks \
        -srcalias localhost -destkeystore cert_and_key.p12 -deststoretype PKCS12
openssl pkcs12 -in cert_and_key.p12 -nocerts -nodes
```
The last command outputs the key to STDOUT. Manualy copy the key into `key.pem`
* extract the CA certificast
```
keytool -exportcert -alias caroot -keystore kafka.truststore.jks -rfc -file caroot.pem
```
* confirm that all 3 files have been created:
```
caroot.pem
certificate.pem
key.pem
```

## Common Java 8 SDK requirements for Server 1 and 2
Follow an [example...](https://www.itzgeek.com/how-tos/linux/debian/how-to-install-oracle-java-8-on-debian-9-ubuntu-linux-mint.html):
* Manually login to [oracle](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and download `jdk-8u241-linux-x64.tar.gz`
* copy the archive to the server `scp jdk-8u241-linux-x64.tar.gz root@ip_server:`
* execute:
```
sudo tar -zxvf jdk-8u221-linux-x64.tar.gz -C /usr/lib/jvm/
sudo update-alternatives --install /usr/bin/java java /usr/lib/jvm/jdk1.8.0_221/bin/java 3
sudo update-alternatives --config java
java -version
```
* modify `~/.profile` and source it with `. ~/.profile`:
```
export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_241/
export JRE_HOME=/usr/lib/jvm/jdk1.8.0_241/jre/
export J2SDKDIR=/usr/lib/jvm/jdk1.8.0_241/
export J2REDIR=/usr/lib/jvm/jdk1.8.0_241/jre/
```

## Common Python requirements for Server 1 and 2
Follow an [example...](https://unix.stackexchange.com/questions/332641/how-to-install-python-3-6)
* build python 3.6.6:
```
wget https://www.python.org/ftp/python/3.6.6/Python-3.6.6.tgz
tar xvf Python-3.6.6.tgz
cd Python-3.6.9
./configure --enable-optimizations --enable-shared --prefix=/usr/local LDFLAGS="-Wl,-rpath /usr/local/lib" --with-ensurepip=install
make -j8
sudo make altinstall
python3.6
```
* modify `~/.profile` and source it with `. ~/.profile`:
```
alias python=python3.6
alias pip=pip3.6
export PYTHON_PATH=/usr/local/bin/
export PATH=$PATH:$PYTHON_PATH
```

## Hadoop cluster for Server 1 and 2
Follow an example[...](https://blog.newnius.com/setup-distributed-hadoop-cluster-with-docker-step-by-step.html)
For hadoop we use the names slave1 and slave2 which corresponds to server1 and server2
* add to `/etc/hosts` 
```
server1_ip hadoop-01
server2_ip hadoop-02
```
* execute on slave1
```
docker swarm init --listen-addr slave1_ip
docker network create --driver overlay hadoop-net
```
* execute on slave2 the command reported by the command above `docker network create..`
```
docker swarm join --token SWMTKN-1—THE-LONG-STRING slave1:2377
```
* execute on slave1 and slave2
```
docker pull newnius/hadoop:2.7.4
sudo mkdir -p /data
sudo chmod 777 /data
mkdir -p /data/hadoop/hdfs/
mkdir -p /data/hadoop/logs/
```
* create hadoop config locally
```
mkdir config
```
* create `./config/core-site.xml`
```
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!-- Put site-specific property overrides in this file. -->
<configuration>
	<property>		 
		<name>fs.defaultFS</name>
		<value>hdfs://hadoop-01:8020</value>
	</property>
	<property>
		<name>fs.default.name</name>
		<value>hdfs://hadoop-01:8020</value>
	</property>
</configuration>
```
* create `./config/hdfs-site.xml`
```
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!-- Put site-specific property overrides in this file. -->
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!-- Put site-specific property overrides in this file. -->
<configuration>
	<property>
		<name>dfs.permissions</name>
		<value>false</value>
	</property>
	<property>
		<name>dfs.namenode.http-address</name>
		<value>hadoop-01:50070</value>
	</property>
	<property>
		<name>dfs.namenode.secondary.http-address</name>
		<value>hadoop-02:50090</value>
	</property>	
	<property>   
		<name>dfs.datanode.max.transfer.threads</name>   
		<value>8192</value>    
	</property>
	<property>
		<name>dfs.replication</name>
		<value>1</value>
	</property>
	<property>
	  	<name>dfs.client.use.datanode.hostname</name>
	  	<value>true</value>
	</property>
</configuration>
```
* create `./config//mapred-site.xml`
```
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!-- Put site-specific property overrides in this file. -->
<configuration>
	<property>	 	        		
		<name>mapreduce.framework.name</name>
		<value>yarn</value>
	</property>
	<property>
		<name>mapreduce.jobhistory.address</name>
		<value>hadoop-01:10020</value>
	</property>
	<property>
		<name>mapreduce.jobhistory.webapp.address</name>
		<value>hadoop-01:19888</value>
	</property>	
</configuration>
```
* create `./config/yarn-site.xml`
```
<?xml version="1.0"?>
<!-- Site specific YARN configuration properties -->
<configuration>
	<property>
		<name>yarn.application.classpath</name>
		<value>/usr/local/hadoop/etc/hadoop, /usr/local/hadoop/share/hadoop/common/*, /usr/local/hadoop/share/hadoop/common/lib/*, /usr/local/hadoop/share/hadoop/hdfs/*, /usr/local/hadoop/share/hadoop/hdfs/lib/*, /usr/local/hadoop/share/hadoop/mapreduce/*, /usr/local/hadoop/share/hadoop/mapreduce/lib/*, /usr/local/hadoop/share/hadoop/yarn/*, /usr/local/hadoop/share/hadoop/yarn/lib/*</value>
	</property>
	<property>
		<name>yarn.resourcemanager.hostname</name>
		<value>hadoop-01</value>
	</property>
	<property>
		<name>yarn.nodemanager.aux-services</name>
		<value>mapreduce_shuffle</value>
	</property>
	<property>
		<name>yarn.log-aggregation-enable</name>
		<value>true</value>
	</property>
	<property>
		<name>yarn.log-aggregation.retain-seconds</name>
		<value>604800</value>
	</property>
	<property>
		<name>yarn.nodemanager.resource.memory-mb</name>
		<value>2048</value>
	</property>
	<property>
		<name>yarn.nodemanager.resource.cpu-vcores</name>
		<value>2</value>
	</property>
	<property>
		<name>yarn.scheduler.minimum-allocation-mb</name>
		<value>1024</value>
	</property>
</configuration>
```
* create `./config/slaves`
```
hadoop-01
hadoop-02
```
*  Distribute the configuration files to all the nodes 
```
scp -r config/ spark@server1:/data/hadoop/
scp -r config/ spark@server2:/data/hadoop/
```
* execute on slave1 to bring up all the nodes:
```
docker service create \
 --name hadoop-01 \
 --hostname hadoop-01 \
 --constraint node.hostname==deflect-baskerville-01 \
 --network hadoop-net \
 --endpoint-mode dnsrr \
 --mount type=bind,src=/data/hadoop/config,dst=/config/hadoop \
 --mount type=bind,src=/data/hadoop/hdfs,dst=/tmp/hadoop-root \
 --mount type=bind,src=/data/hadoop/logs,dst=/usr/local/hadoop/logs \
 newnius/hadoop:2.7.4

 docker service create \
 --name hadoop-02 \
 --hostname hadoop-02 \
 --constraint node.hostname==deflect-baskerville-01 \
 --network hadoop-net \
 --endpoint-mode dnsrr \
 --mount type=bind,src=/data/hadoop/config,dst=/config/hadoop \
 --mount type=bind,src=/data/hadoop/hdfs,dst=/tmp/hadoop-root \
 --mount type=bind,src=/data/hadoop/logs,dst=/usr/local/hadoop/logs \
 newnius/hadoop:2.7.4
```
* login to Hadoop container
```
docker exec -it hadoop-01.1.$(docker service ps hadoop-01 --no-trunc | tail -n 1 | awk '{print $1}') bash
```
* execute in Hadoop container
```
# Stop all Hadoop processes 
sbin/stop-yarn.sh
sbin/stop-dfs.sh
# Format namenode 
bin/hadoop namenode -format
# Start yarn and hdfs nodes 
sbin/start-dfs.sh
sbin/start-yarn.sh
# Start historyserver
sbin/mr-jobhistory-daemon.sh start historyserver
# Verify
hdfs dfsadmin -report
```
* Expose HDFS. Execute on server1
```
docker service create \
        --name hadoop-01-forwarder-hdfs \
        --constraint node.hostname==deflect-baskerville-01 \
        --network hadoop-net \
        --env REMOTE_HOST=hadoop-01 \
        --env REMOTE_PORT=8020 \
        --env LOCAL_PORT=8020 \
        --publish mode=host,published=8020,target=8020 \
        newnius/port-forward

docker service create \
        --name hadoop-01-forwarder \
        --constraint node.hostname==deflect-baskerville-01 \
        --network hadoop-net \
        --env REMOTE_HOST=hadoop-01 \
        --env REMOTE_PORT=50070 \
        --env LOCAL_PORT=50070 \
        --publish mode=host,published=50070,target=50070 \
        newnius/port-forward

docker service create \
        --name hadoop-02-forwarder \
        --constraint node.hostname==deflect-baskerville-02 \
        --network hadoop-net \
        --env REMOTE_HOST=hadoop-02 \
        --env REMOTE_PORT=50075 \
        --env LOCAL_PORT=50075 \
        --publish mode=host,published=50075,target=50075 \
        newnius/port-forward
        
docker service create \
        --name hadoop-02-forwarder2 \
        --constraint node.hostname==deflect-baskerville-02 \
        --network hadoop-net \
        --env REMOTE_HOST=hadoop-02 \
        --env REMOTE_PORT=50010\
        --env LOCAL_PORT=50010 \
        --publish mode=host,published=50010,target=50010 \
        newnius/port-forward

```
* Expose Web UI. Execute on server1
```
docker service create \
	--name hadoop-proxy \
	--hostname hadoop-proxy \
	--network hadoop-net \
	--replicas 1 \
	--detach=true \
	--publish 7001:7001 \
	newnius/docker-proxy
``` 

## Common Spark requirements for Server 1 and 2
* install [spark](https://www.programcreek.com/2018/11/install-spark-on-ubuntu-standalone-mode)
```
adduser spark
sudo usermod -aG sudo spark
su spark
wget http://apache.claz.org/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz
tar -xvf spark-2.4.0-bin-hadoop2.7.tgz
sudo mv spark-2.4.0-bin-hadoop2.7 /usr/local/
sudo ln -s /usr/local/spark-2.4.0-bin-hadoop2.7/ /usr/local/spark
```
* modify `.profile` and source it with `. ~/.profile`:
```
export SPARK_HOME=/usr/local/spark
export PATH=$PATH:$SPARK_HOME/bin
```
* add to `/usr/local/spark/conf/spark-env`:
```
SPARK_MASTER_HOST="server1_ip"
PYSPARK_PYTHON="/usr/local/bin/python3.6"
```
* Rename `/usr/local/spark/conf/spark-env.sh.template' to `/usr/local/spark/conf/spark-enf.sh'
* Set up master node IP in `/usr/local/spark/conf/spark-enf.sh`
```
SPARK_MASTER_HOST="server1_ip"
```
* Edit `/etc/hosts`:
```
server1_ip slave1
server2_ip slave2
```

## Server 1. Spark master.
* Login as user `spark`
* Set up password-less SSH to both workers. See [example...](https://www.programcreek.com/2018/11/install-spark-on-ubuntu-standalone-mode/)
```
ssh-keygen -t rsa
ssh-copy-id spark@server1_ip
ssh-copy-id spark@server2_ip
```
* Rename `/usr/local/spark/conf/slaves.template` to `/usr/local/spark/conf/slaves`
* Define slaves in `/usr/local/spark/conf/slaves`:
```
slave1
slave2
```
* Start spark master `start-master.sh`
* Start spark slaves `start-slaves.sh`
* Run spark shell to confirm spark is up and running: 
```
spark-shell --master spark://server1_ip:7077
```

## Baskerville for Server 1 and Server 2
* login as user spark
* clone and install [Esretriever](https://github.com/equalitie/esretriever) 
```
git clone https://github.com/equalitie/esretriever.git
cd esretriever
git checkout issue_17_dockerize
git pull
sudo pip install -e .
cd ..
```
* clone and install [Basekrville](https://github.com/equalitie/baskerville)
```
git clone https://github.com/equalitie/baskerville.git
cd baskerville
sudo pip install -e .
```
* set up a cron job for Java GC memory leak workaround
```
/home/spark/baskerville/data/set_up_cron_job_for_periodic_gc.sh
```

## Baskerville configuration for Server 1
* login as user spark
* create the log folder:
```
mkdir /home/spark/baserville/src/baskerville/logs
```
* rename `~/baskerville/conf/conf_example_baskerville.yaml' to `~/basekrville/conf/baskerville.yaml`
* modify `~/baskerville/conf/baskerville.yaml`:
```
database:
	name: baskerville                   # the database name
	user: !ENV ${DB_USER}
	password: !ENV '${DB_PASS}'
	type: 'postgres'
	host: server3_ip
	port: !ENV ${DB_PORT}
	maintenance:                        # Optional, for data partitioning and archiving
		partition_table: 'request_sets'   # default value
		partition_by: week                # partition by week or month, default value is week
		partition_field: created_at       # which field to use for the partitioning, this is the default value, can be omitted
		strict: False                     # if False, then for the week partition the start and end date will be changed to the start and end of the respective weeks. If true, then the dates will remain unchanged. Be careful to be consistent with this.
		data_partition:                   # Optional: Define the period to create partitions for
			since: 2020-01-01               # when to start partitioning
			until: "2020-12-31 23:59:59"    # when to stop partitioning
kafka:
	url: server2_ip:9092
engine:
	training:
	    train_global_model: False
	    training_days: 14
	    n_estimators: 1000
	    max_samples: 1.0
	    contamination: 0.2
	logpath: './logs/log.txt'
	log_level: 'DEBUG'
  	manual_raw_log:
  		raw_log_paths:
  			- '/home/spark/baskerville/data/samples/test_data_1k.json'
  	data_config:
  		schema: '/home/spark/baskerville/data/samples/ats_log_schema.json'
  	extra_features:                
	    - 'request_rate'
	    - 'css_to_html_ratio'
	    - 'image_to_html_ratio'
	    - 'js_to_html_ratio'
	    - 'minutes_total'
	    - 'path_depth_average'
	    - 'path_depth_variance'
	    - 'payload_size_average'
	    - 'payload_size_log_average'
	    - 'request_interval_average'
	    - 'request_interval_variance'
	    - 'request_total'
	    - 'response4xx_to_request_ratio'
	    - 'top_page_to_request_ratio'
	    - 'unique_path_rate'
	    - 'unique_path_to_request_ratio'
	    - 'unique_query_rate'
	    - 'unique_query_to_unique_path_ratio'
	    - 'unique_ua_rate'
manual_es:
	batch_length: 120
	save_logs_dir: '/home/spark/es'
spark:
	log_level: 'ERROR'
	#metrics_conf:
	jars: '/home/spark/baskerville/data/jars/spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar,/home/spark/baskerville/data/jars/postgresql-42.2.4.jar,/home/spark/equalitie/baskerville/data/jars/elasticsearch-spark-20_2.11-5.6.5.jar'
```

## Testing Pipelines
* Execute Raw logs pipeline:
```
cd /home/spark/baskerville/src/baskerville
spark-submit --master spark://$SPARK_MASTER_HOST:7077 --jars '/home/spark/baskerville/data/jars/spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar,/home/spark/baskerville/data/jars/postgresql-42.2.4.jar,/home/spark/baskerville/data/jars/elasticsearch-spark-20_2.11-5.6.5.jar' main.py rawlog
```
* Monitor the logs and confirm you see the command `Saving request_sets` is executed successfully.
* Execute Kafka pipeline:
```
cd /home/spark/baskerville/src/baskerville
spark-submit --master spark://$SPARK_MASTER_HOST:7077 --jars '/home/spark/baskerville/data/jars/spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar,/home/spark/baskerville/data/jars/postgresql-42.2.4.jar,/home/spark/baskerville/data/jars/elasticsearch-spark-20_2.11-5.6.5.jar' main.py kafka -s
```
* Monitor the logs and confirm you see the command `Saving request_sets` is executed successfully.

## Logstash Kafka Output
* Stop logstash:
```
sudo server logstash stop
```
* Install kafka output logstash plugin
```
sudo /usr/share/logstash/bin/logstash-plugin install logstash-output-kafka
sudo /usr/share/logstash/bin/logstash-plugin update logstash-output-kafka
sudo mkdir /etc/logstash/ssl_kafka
```
* Copy the client storekey and the truststore to logstash server
```
scp -oProxyCommand="ssh -At -W %h:%p your_user_name@cerveaux.prod.deflect.ca" kafka.client.keystore.jks sysop@opsdashca0.deflect.ca:/etc/logstash/ssl_kafka
scp -oProxyCommand="ssh -At -W %h:%p your_user_name@cerveaux.prod.deflect.ca" kafka.truststore.jks sysop@opsdashca0.deflect.ca:/etc/logstash/ssl_kafka
```
* Configure kafka output in `/etc/logstash/conf.d/edgelogs.conf`:
```
output {
	#...
	kafka {
	    id => "baskerville"
	    topic_id => "deflect.logs"
	    acks => "0"
	    bootstrap_servers => "kafka_server_ip:9092"
	    security_protocol => "SSL"
	    ssl_key_password => "user password created in Kafka section"
	    ssl_keystore_location => "/etc/logstash/ssl_kafka/kafka.client.keystore.jks"
	    ssl_keystore_password => "user password created in Kafka section"
	    ssl_keystore_type => "JKS"
	    ssl_truststore_location => "/etc/logstash/ssl_kafka/kafka.truststore.jks"
	    ssl_truststore_password => "spark user password"
	    ssl_truststore_type => "JKS"
	    ssl_endpoint_identification_algorithm => ""
  	}
}
```
* Start logstash:
```
sudo server logstash start
```
* Confirm that Kafka is receving the logs(see above example of testing Kafka consumer):
```
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server kafka_server_ip:9092 --topic deflect.logs --consumer.config client-ssl.properties
```
## Start Baskerville
* Start Kafka pipeline
```
cd /home/spark/baskerville/src/baskerville

spark-submit --master spark://server1:7077 --jars /home/spark/baskerville/data/jars/spark-streaming-kafka-0-8-assembly_2.11-2.4.4.jar,/home/spark/baskerville/data/jars/postgresql-42.2.4.jar,/home/spark/baskerville/data/jars/elasticsearch-spark-20_2.11-5.6.5.jar main.py kafka -e &>/dev/null &
```
* Monitor Baskerville logs
```
tail -f /home/spark/baskerville/src/baskerville/logs/log.txt
```
* Monitor Spark UI at `http://server1_ip:8080/`

## Logstash Kafka Output
* Stop logstash:
```
sudo server logstash stop
```
* Install kafka output logstash plugin
```
sudo /usr/share/logstash/bin/logstash-plugin install logstash-output-kafka
sudo /usr/share/logstash/bin/logstash-plugin update logstash-output-kafka
sudo mkdir /etc/logstash/ssl_kafka
```
* Copy the client storekey and the truststore to logstash server
```
scp -oProxyCommand="ssh -At -W %h:%p your_user_name@cerveaux.prod.deflect.ca" kafka.client.keystore.jks sysop@opsdashca0.deflect.ca:/etc/logstash/ssl_kafka
scp -oProxyCommand="ssh -At -W %h:%p your_user_name@cerveaux.prod.deflect.ca" kafka.truststore.jks sysop@opsdashca0.deflect.ca:/etc/logstash/ssl_kafka
```
* Configure kafka output in `/etc/logstash/conf.d/edgelogs.conf`:
```
output {
	#...
	kafka {
	    id => "baskerville"
	    topic_id => "deflect.logs"
	    acks => "0"
	    bootstrap_servers => "kafka_server_ip:9092"
	    security_protocol => "SSL"
	    ssl_key_password => "user password created in Kafka section"
	    ssl_keystore_location => "/etc/logstash/ssl_kafka/kafka.client.keystore.jks"
	    ssl_keystore_password => "user password created in Kafka section"
	    ssl_keystore_type => "JKS"
	    ssl_truststore_location => "/etc/logstash/ssl_kafka/kafka.truststore.jks"
	    ssl_truststore_password => "spark user password"
	    ssl_truststore_type => "JKS"
	    ssl_endpoint_identification_algorithm => ""
  	}
}
```
* Start logstash:
```
sudo server logstash start
```
* Confirm that Kafka is receving the logs(see above example of testing Kafka consumer):
```
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server kafka_server_ip:9092 --topic deflect.logs --consumer.config client-ssl.properties
```



<a rel="license" href="http://creativecommons.org/licenses/by/4.0/">
<img alt="Creative Commons Licence" style="border-width:0" src="https://i.creativecommons.org/l/by/4.0/80x15.png" /></a><br />
This work is copyright (c) 2020, eQualit.ie inc., and is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by/4.0/">Creative Commons Attribution 4.0 International License</a>.