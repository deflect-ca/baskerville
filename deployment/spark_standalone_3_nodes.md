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

## Kafka keys
* execute on bnode1
* create directory `mkdir ~/kafka `
* generate TLS truststore with CA root certificate
IMPORTANT: when prompted tor First and Last name, type: `Baskerville`
```
cd ~/kafka
git clone https://github.com/confluentinc/confluent-platform-security-tools
cd /confluent-platform-security-tools
export COUNTRY=CA
export STATE=QC
export ORGANIZATION_UNIT=eQualitie
export CITY=Montreal
export PASSWORD=password_kafka
bash ./kafka-generate-ssl-automatic.sh
mkdir ../keys
rm -r keystore 
```
* generate keys for the nodes and a client. 
Answer no to the first question(you do not want to recreate the truststore). 
Type './truststore/kafka.truststore.jks' when prompted for truststore.
Type `./truststore/kafka.truststore.jks` when prompted for truststore private key.
You can also use a new client password. 
IMPORTANT: when prompted tor First and Last name(except for the client key), type the nodes IP respectively.


```
bash ./kafka-generate-ssl.sh
mv keystore ../keys/keystore1
bash ./kafka-generate-ssl.sh
mv keystore ../keys/keystore2
bash ./kafka-generate-ssl.sh
mv keystore ../keys/keystore3
bash ./kafka-generate-ssl.sh
mv keystore ../keys/keystore_client
mv truststore ../keys/truststore
```

* download the keys locally
```
scp -r -oProxyJump=your_user_name@cerveaux.prod.deflect.ca root@bnode1.deflect.ca:/root/kafka/keys ./
```
* copy the keys to bnode2 bnode3
```
scp -r -oProxyJump=your_user_name@cerveaux.prod.deflect.ca ./keys root@bnode2.deflect.ca:/root/kafka/
scp -r -oProxyJump=your_user_name@cerveaux.prod.deflect.ca ./keys root@bnode3.deflect.ca:/root/kafka/
```

## Kafka
* execute on bnode1, bnode2, bnode3
* For brokers follow [this repo...](https://github.com/bitnami/bitnami-docker-kafka)
* For zookeeper ensemble follow [this repo...](https://hub.docker.com/r/bitnami/zookeeper/)
* create volumes
```
mkdir kafka
cd ~/kafka
mkdir zookeeper
mkkir zookeepr/volume
```
* set password variable `vi ~/.profile` and source it with `. ~/.profile`:
```
export KAFKA_PASSWORD=...
```

* Create docker-compose file `vi docker-compose.yaml`.
Below is the example for bnode1:
```
---
version: "3.1"
services:
  zookeeper:
    user: root
    image: 'bitnami/zookeeper:latest'
    restart: always
    hostname: bnode1.deflect.ca
    container_name: bnode1.deflect.ca
    #network_mode: host
    ports:
      - '3181:3181'
      - '2888:2888'
      - '3888:3888'
    volumes:
      - /root/kafka/zookeeper/volume:/bitnami/zookeeper
      - /root/kafka/keys/keystore1/kafka.keystore.jks:/bitnami/zookeeper/certs/keystore.jks
      - /root/kafka/keys/truststore/kafka.truststore.jks:/bitnami/zookeeper/certs/truststore.jks

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
```
* for bnode2 change 
```
      - /root/kafka/keys/keystore2/kafka.keystore.jks:/bitnami/zookeeper/certs/keystore.jks
      - ZOO_SERVER_ID=2
      - ZOO_SERVERS=bnode1.deflect.ca:2888:3888,0.0.0.0:2888:3888,bnode3.deflect.ca:2888:3888
```
* for bnode3 change 
```
      - /root/kafka/keys/keystore3/kafka.keystore.jks:/bitnami/zookeeper/certs/keystore.jks
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

