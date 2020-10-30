# Baskerville deployment
This file is an example of 3-nodes deployment with Spark in standalone mode.

## General tools
* execute on bnode1, bnode2, bnode3

* Docker Compose
```
sudo curl -L "https://github.com/docker/compose/releases/download/1.27.4/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```
* wget `sudo apt-get install wget`


## JAVA 8 sdk (bnode1, bnode2, bnode3)
* execute on bnode1, bnode2, bnode3
* Follow an [example...](https://www.itzgeek.com/how-tos/linux/debian/how-to-install-oracle-java-8-on-debian-9-ubuntu-linux-mint.html):
* Manually login to [oracle](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and download `jdk-8u241-linux-x64.tar.gz`
* copy the archive to the server 
`scp -oProxyJump=your_user_name@cerveaux.prod.deflect.ca ~/Downloads/jdk-8u241-linux-x64.tar.gz root@bnode1.deflect.ca:`
* execute:
```
sudo mkdir /usr/lib/jvm
sudo tar -zxvf jdk-8u241-linux-x64.tar.gz -C /usr/lib/jvm/
sudo update-alternatives --install /usr/bin/java java /usr/lib/jvm/jdk1.8.0_241/bin/java 3
sudo update-alternatives --config java
java -version
```
* modify `vi ~/.profile` and source it with `. ~/.profile`:
```
export PATH=$PATH:/usr/lib/jvm/jdk1.8.0_241/bin
export JAVA_HOME=/usr/lib/jvm/jdk1.8.0_241/
export JRE_HOME=/usr/lib/jvm/jdk1.8.0_241/jre/
export J2SDKDIR=/usr/lib/jvm/jdk1.8.0_241/
export J2REDIR=/usr/lib/jvm/jdk1.8.0_241/jre/
```

## SSL keys
* execute on bnode1
* generate TLS truststore with CA root certificate
IMPORTANT: when prompted tor First and Last name, type: `Baskerville`
```
mkdir ~/keys
git clone https://github.com/confluentinc/confluent-platform-security-tools
cd /confluent-platform-security-tools
export COUNTRY=CA
export STATE=QC
export ORGANIZATION_UNIT=eQualitie
export CITY=Montreal
export PASSWORD=password_kafka
bash ./kafka-generate-ssl-automatic.sh
rm -r keystore 
cp truststore ../keys/truststore
echo $PASSWORD >~/keys/creds
```
* generate ZOOKEPER keys  
Answer no to the first question(you do not want to recreate the truststore). 
Type './truststore/kafka.truststore.jks' when prompted for truststore.
Type `./truststore/kafka.truststore.jks` when prompted for truststore private key.
You can also use a new client password. 
IMPORTANT: when prompted tor First and Last name(except for the client key), 
type the nodes **IP** respectively.

```
bash ./kafka-generate-ssl.sh
mv keystore ../keys/zookeeper/keystore1
bash ./kafka-generate-ssl.sh
mv keystore ../keys/zookeeper/keystore2
bash ./kafka-generate-ssl.sh
mv keystore ../keys/zookeeper/keystore3
```

* generate KAFKA keys  
Answer no to the first question(you do not want to recreate the truststore). 
Type './truststore/kafka.truststore.jks' when prompted for truststore.
Type `./truststore/kafka.truststore.jks` when prompted for truststore private key.
You can also use a new client password. 
IMPORTANT: when prompted tor First and Last name(except for the client key), 
type the nodes **DNS** respectively.

```
bash ./kafka-generate-ssl.sh
mv keystore ../keys/kafka/keystore1
bash ./kafka-generate-ssl.sh
mv keystore ../keys/kafka/keystore2
bash ./kafka-generate-ssl.sh
mv keystore ../keys/kafka/keystore3
```

* download the keys locally
```
scp -r -oProxyJump=your_user_name@cerveaux.prod.deflect.ca root@bnode1.deflect.ca:/root/keys ./
```
* copy the keys to bnode2 bnode3
```
scp -r -oProxyJump=your_user_name@cerveaux.prod.deflect.ca ./keys root@bnode2.deflect.ca:/root/
scp -r -oProxyJump=your_user_name@cerveaux.prod.deflect.ca ./keys root@bnode3.deflect.ca:/root/
```

## Zookeeper
* execute on bnode1, bnode2, bnode3
* For zookeeper ensemble follow [this repo...](https://hub.docker.com/r/bitnami/zookeeper/)
```
mkdir ~/zookeeper
cd ~/zookeeper
mkdir volume
```
* set password variable `vi ~/.profile` and source it with `. ~/.profile`:
```
export KAFKA_PASSWORD=...
```
* Create docker-compose file `vi docker-compose.yaml`.
Below is the example for bnode1:

```
version: "3.1"
services:
  zookeeper:
    user: root
    image: 'bitnami/zookeeper:latest'
    tmpfs: "/datalog"
    restart: always
    hostname: bnode1.deflect.ca
    container_name: zookeeper
    #network_mode: host
    ports:
      - '3181:3181'
      - '2888:2888'
      - '3888:3888'
    volumes:
      - /root/zookeeper/volume:/bitnami/zookeeper
      - /root/keys/zookeeper/keystore1/kafka.keystore.jks:/bitnami/zookeeper/certs/keystore.jks
      - /root/keys/truststore/kafka.truststore.jks:/bitnami/zookeeper/certs/truststore.jks
    environment:
      - ZOO_ENABLE_AUTH=True
      - ZOO_TLS_CLIENT_ENABLE=True
      - ZOO_TLS_PORT_NUMBER=3181
      - ZOO_TLS_CLIENT_KEYSTORE_FILE=/bitnami/zookeeper/certs/keystore.jks
      - ZOO_TLS_CLIENT_KEYSTORE_PASSWORD=${KAFKA_PASSWORD}
      - ZOO_TLS_CLIENT_TRUSTSTORE_FILE=/bitnami/zookeeper/certs/truststore.jks
      - ZOO_TLS_CLIENT_TRUSTSTORE_PASSWORD=${KAFKA_PASSWORD}
      - ZOO_TLS_QUORUM_ENABLE=True
      - ZOO_TLS_QUORUM_KEYSTORE_FILE=/bitnami/zookeeper/certs/keystore.jks
      - ZOO_TLS_QUORUM_KEYSTORE_PASSWORD=${KAFKA_PASSWORD}
      - ZOO_TLS_QUORUM_TRUSTSTORE_FILE=/bitnami/zookeeper/certs/truststore.jks
      - ZOO_TLS_QUORUM_TRUSTSTORE_PASSWORD=${KAFKA_PASSWORD}
      - ZOO_SERVER_ID=1
      - ZOO_SERVERS=0.0.0.0:2888:3888,bnode2.deflect.ca:2888:3888,bnode3.deflect.ca:2888:3888
      - ZOO_SERVER_USERS=baskerville
      - ZOO_SERVER_PASSWORDS=${KAFKA_PASSWORD}
      - ZOO_CLIENT_USERS=baskerville
      - ZOO_CLIENT_PASSWORD=${KAFKA_PASSWORD}      
```

* for bnode2 change 

```
      - /root/keys/keystore2/kafka.keystore.jks:/bitnami/zookeeper/certs/keystore.jks
      - ZOO_SERVER_ID=2
      - ZOO_SERVERS=bnode1.deflect.ca:2888:3888,0.0.0.0:2888:3888,bnode3.deflect.ca:2888:3888
```
* for bnode3 change 
```
      - /root/keys/keystore3/kafka.keystore.jks:/bitnami/zookeeper/certs/keystore.jks
      - ZOO_SERVER_ID=3
      - ZOO_SERVERS=bnode1.deflect.ca:2888:3888,bnode2.deflect.ca:2888:3888,0.0.0.0:2888:3888
```

* Fix for bitnami permission issue
`sudo docker-compose run --user="root" --entrypoint chown zookeeper -R 1001 /bitnami`

* open firewall ports
```
sudo ufw allow 3181
sudo ufw allow 2888
sudo ufw allow 3888
```
* `docker-compose up -d`

## Kafka
* execute on bnode1, bnode2, bnode3
* For brokers follow [this repo...](https://github.com/bitnami/bitnami-docker-kafka)
* copy certificates for kafka broker
```
cd ~/kafka
mkdir config
mkdir config/certs
cp ~/keys/truststore/kafka.truststore.jks config/certs/zookeeper.truststore.jks 
```
for bnode1 : `cp ~/keys/keystore1/kafka.keystore.jks config/certs/zookeeper.keystore.jks`
for bnode2 : `cp ~/keys/keystore2/kafka.keystore.jks config/certs/zookeeper.keystore.jks`
for bnode3 : `cp ~/keys/keystore3/kafka.keystore.jks config/certs/zookeeper.keystore.jks`
* Create docker-compose file `vi docker-compose.yaml`.
Below is the example for bnode1:
```
version: "3.1"
services:
  kafka:
    user: root
    image: 'bitnami/kafka:latest'
    network_mode: host
    restart: always
    ports:
      - '9092:9092'
      - '9093:9093'
      - '9094:9094'
    volumes:
      - /root/keys/kafka/keystore1/kafka.keystore.jks:/bitnami/kafka/config/certs/zookeeper.keystore.jks:ro
      - /root/keys/kafka/keystore1/kafka.keystore.jks:/bitnami/kafka/config/certs/kafka.keystore.jks:ro
      - /root/keys/truststore/kafka.truststore.jks:/bitnami/kafka/config/certs/zookeeper.truststore.jks:ro
      - /root/keys/truststore/kafka.truststore.jks:/bitnami/kafka/config/certs/kafka.truststore.jks:ro
      - /root/kafka/logs:/opt/bitnami/kafka/logs/
      - /root/kafka/config/kafka_jaas.conf:/opt/bitnami/kafka/config/kafka_jaas.conf
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_ZOOKEEPER_PROTOCOL=SASL_SSL
      - KAFKA_ZOOKEEPER_CONNECT=bnode1.deflect.ca:3181,bnode2.deflect.ca:3181,bnode3.deflect.ca:3181
      - KAFKA_ZOOKEEPER_TLS_KEYSTORE_PASSWORD=${KAFKA_PASSWORD}
      - KAFKA_ZOOKEEPER_TLS_TRUSTSTORE_PASSWORD=${KAFKA_PASSWORD}

      - KAFKA_LOG_RETENTION_MUNUTES=20
      - KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS=120000
      - ALLOW_PLAINTEXT_LISTENER=True
      - KAFKA_CFG_SSL_CLIENT_AUTH=required
      - KAFKA_CFG_MIN_INSYNC_REPLICAS=2
      - KAFKA_CFG_DEFAULT_REPLICATION_FACTOR=3
      - KAFKA_CFG_SASL_ENABLED_MECHANISMS=PLAIN
      - KAFKA_CFG_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN

      - KAFKA_CFG_LISTENERS=PLAIN://:9092,ENCRYPTED://:9093,ENCRYPTED_NOUA://:9094
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAIN://bnode1.deflect.ca:9092,ENCRYPTED://bnode1.deflect.ca:9093,ENCRYPTED_NOUA://bnode1.deflect.ca:9094
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=PLAIN:SASL_PLAINTEXT,ENCRYPTED:SASL_SSL,ENCRYPTED_NOUA:SSL
      - KAFKA_INTER_BROKER_LISTENER_NAME=ENCRYPTED
      - KAFKA_CFG_SECURITY_INTER_BROKER_PROTOCOL=SASL_SSL
      - KAFKA_OPTS= #-Djavax.net.debug=all

      - KAFKA_CFG_SSL_KEYSTORE_LOCATION=/bitnami/kafka/config/certs/kafka.keystore.jks
      - KAFKA_CFG_SSL_KEYSTORE_PASSWORD=${KAFKA_PASSWORD}
      - KAFKA_CFG_SSL_KEY_PASSWORD=${KAFKA_PASSWORD}
      - KAFKA_CFG_SSL_TRUSTSTORE_LOCATION=/bitnami/kafka/config/certs/kafka.truststore.jks
      - KAFKA_CFG_SSL_TRUSTSTORE_PASSWORD=${KAFKA_PASSWORD}
```

* for bnode2 change `keystore1` to `keystore2` and `bnode1` to `bnode2`
* for bnode3 change `keystore1` to `keystore2` and `bnode1` to `bnode2`

* Fix for bitnami permission issue
`sudo docker-compose run --user="root" --entrypoint chown kafka -R 1001 /bitnami`

* Creat JAAS config `vi ~/kafka/config/kafka_jaas.conf`:
```
Client {
   org.apache.kafka.common.security.plain.PlainLoginModule required
   username="baskerville"
   password="kafka_password";
   };
KafkaServer {
   org.apache.kafka.common.security.plain.PlainLoginModule required
   username="baskerville"
   password="kafka_password"
   user_baskerville="kafka_password";
   };
KafkaClient {
   org.apache.kafka.common.security.plain.PlainLoginModule required
   username="baskerville"
   password="kafka_password";
   };
```
* open firewall ports
```
sudo ufw allow 9092
sudo ufw allow 9093
sudo ufw allow 9094
```
* `docker-compose up -d`

* create a local ssl configration file
```
echo "bootstrap.servers=bnode1.deflect.ca:9093,bnode2.deflect.ca:9093,bnode3.deflect.ca:9093
security.protocol=SASL_SSL
ssl.endpoint.identification.algorithm=
ssl.truststore.location=./keys/truststore/kafka.truststore.jks
ssl.truststore.password=password_kafka
ssl.keystore.location=./keys/keystore_client/kafka.keystore.jks
ssl.keystore.password=password_client
ssl.key.password=password_client
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="baskerville" password="B1^ZRUUVoIuKND7t2HiJ8fwRg0kdMo4zdh8m8eRzgXw!";
">> ssl.properties
```

* test kafka producer 
```
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list bnode1.deflect.ca:9093 --topic piper --producer.config ssl.properties```
```

* test kafka consumer
```
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server bnode1.deflect.ca:9093,bnode2.deflect.ca:9093,bnode3.deflect.ca:9093 --topic piper128 --consumer.config ssl.properties
```

* extract the keys in PEM format for use in python:
```
keytool -exportcert -alias localhost -keystore ./keys/keystore_client/kafka.keystore.jks -rfc -file certificate.pem
keytool -v -importkeystore -srckeystore ./keys/keystore_client/kafka.keystore.jks \
        -srcalias localhost -destkeystore cert_and_key.p12 -deststoretype PKCS12
openssl pkcs12 -in cert_and_key.p12 -nocerts -nodes
```
The last command outputs the key to STDOUT. Manualy copy the key into `key.pem`
* extract the CA certificast
```
keytool -exportcert -alias caroot -keystore ./keys/truststore/kafka.truststore.jks -rfc -file caroot.pem
```
* confirm that all 3 files have been created:
```
caroot.pem
certificate.pem
key.pem
```

## Logstash Kafka Output
* Stop logstash:
```
ssh -At -W %h:%p your_user_name@cerveaux.prod.deflect.ca
sudo server logstash stop
```
* Install kafka output logstash plugin
```
sudo /usr/share/logstash/bin/logstash-plugin install logstash-output-kafka
sudo /usr/share/logstash/bin/logstash-plugin update logstash-output-kafka
sudo mkdir /etc/logstash/ssl_kafka_greenhost
```
* copy the keys
```
scp -oProxyCommand="ssh -At -W %h:%p anton@cerveaux.prod.deflect.ca" ./keys/keystore_client/kafka.keystore.jks sysop@opsdashca0.deflect.ca:
scp -oProxyCommand="ssh -At -W %h:%p anton@cerveaux.prod.deflect.ca" ./keys/truststore/kafka.truststore.jks sysop@opsdashca0.deflect.ca:
ssh -At your_user_name@cerveaux.prod.deflect.ca ssh sysop@opsdashca0.deflect.ca
sudo mv kafka.keystore.jks /etc/logstash/ssl_kafka_greenhost
sudo mv kafka.truststore.jks /etc/logstash/ssl_kafka_greenhost
```
* create jaas config `vi /etc/logstash/jaas.conf`
```
KafkaClient {
org.apache.kafka.common.security.plain.PlainLoginModule required username="baskerville" password="kafka_password";
};
```
* add kaka output
```
  if [type] == "deflect_access" {
    kafka {
      id => "baskerville_greenhost"
      topic_id => "deflect.logs"
      acks => "0"
      codec => "json"
      message_key => "%{host}"
      batch_size => 500000
      bootstrap_servers => "bnode1.deflect.ca:9093,bnode2.deflect.ca:9093,bnode3.deflect.ca:9093"
      security_protocol => "SASL_SSL"
      ssl_key_password => "kafka_password"
      ssl_keystore_location => "/etc/logstash/ssl_kafka_greenhost/kafka.keystore.jks"
      ssl_keystore_password => "kafka_password"
      ssl_keystore_type => "JKS"
      ssl_truststore_location => "/etc/logstash/ssl_kafka_greenhost/kafka.truststore.jks"
      ssl_truststore_password => "kafka_password"
      ssl_truststore_type => "JKS"
      sasl_mechanism=> "PLAIN"
      security_protocol=> "SASL_SSL"
      jaas_path=> "/etc/logstash/jaas.conf"
    }
  }
```

* restart logstash
```
sudo service logstash restart
```

* confirm Kafka gets the logs
```
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server bnode1.deflect.ca:9093,bnode2.deflect.ca:9093,bnode3.deflect.ca:9093 --topic deflect.logs --consumer.config ssl.properties
```