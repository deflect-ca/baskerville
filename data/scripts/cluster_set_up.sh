#!/usr/bin/env bash
export USER=maria

export PYTHON3_VERSION=3.6.8
export SPARK_VERSION=2.4.0
export SSH_KEY="~/.ssh/id_rsa.pub"
export SPARK_HOME=/home/maria/spark/spark-${SPARK_VERSION}-bin-hadoop2.7
export IP=$(ip route get 1 | awk '{print $NF;exit}')
export SPARK_MASTER_HOST=${IP}
export PYSPARK_PYTHON=/usr/local/bin/python3.6
export PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.6
export BASKERVILLE_ROOT=/home/maria/baskerville
export KAFKA_HOST=kafka-dev.localdomain
export POSTGRES_HOST=127.0.0.1
export POSTGRES_USER=baskerville_online
export POSTGRES_PASS=b@skervi11e
export BASKERVILLE_DB=test_baskerville_spark_machine
export GIT_DEPLOY_TOKEN=qw-RniByPwk5JAwUrXDq
export GIT_DEPLOY_USER=gitlab+deploy-token-2

# get spark
mkdir /opt/spark
cd /opt/spark
wget https://www-us.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
tar -xvzf spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

# Python 3.6.x:
sudo apt-get update && sudo apt-get upgrade
sudo apt-get install -y make build-essential libssl-dev zlib1g-dev
sudo apt-get install -y libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm
sudo apt-get install -y libncurses5-dev  libncursesw5-dev xz-utils tk-dev
wget https://www.python.org/ftp/python/${PYTHON3_VERSION}/Python-${PYTHON3_VERSION}.tgz
tar xvf Python-${PYTHON3_VERSION}.tgz
cd Python-${PYTHON3_VERSION}
./configure --enable-optimizations
make -j8
sudo make altinstall
python3.6

# get and install baskerville and dependencies
cd $HOME
git clone https://$GIT_DEPLOY_USER:$GIT_DEPLOY_TOKEN@gitlab.internal.equalit.ie/deflect/baskerville.git

cd baskerville
pip3 install . --process-dependency-links

# Set up cluster
# Set up spark-defaults.conf & spark-defaults.sh:
cp spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh.template spark-2.4.0-bin-hadoop2.7/conf/spark-env.sh
cp spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf.template spark-2.4.0-bin-hadoop2.7/conf/spark-defaults.conf
echo "SPARK_WORKER_CORES=2
SPARK_WORKER_INSTANCES=4" > $SPARK_HOME/conf/spark_env.sh
echo "spark.driver.host      ${IP}
spark.driver.port		 5555" > $SPARK_HOME/conf/spark-defaults.conf

# Start Master:
$SPARK_HOME/sbin/start-master.sh -h $SPARK_MASTER_HOST --properties-file $SPARK_HOME/conf/spark-defaults.conf
# Start Workers:
$SPARK_HOME/sbin/start-slave.sh spark://$SPARK_MASTER_HOST:7077 --properties-file $SPARK_HOME/conf/spark-defaults.conf

# set up screens for:
# Kafka
# use your account to add the spark machine's ssh key
ssh -i ~/.ssh/mykeyforkafka ${USER}@188.166.75.192
echo cat $SSH_KEY>> ~/.ssh/authorized_keys
# then tunnel from the spark machine
ssh -i $SSH_KEY -L 9092:127.0.0.1:9092 -L 2181:127.0.0.1:2181 ${USER}@188.166.75.192
# PG
screen -d -m ssh -i ~/.ssh/mykeyforpg ${USER}@pg.internal.equalit.ie
echo cat $SSH_KEY>> ~/.ssh/authorized_keys
# then tunnel from the spark machine
screen -d -m ssh -i $SSH_KEY -L 9092:127.0.0.1:9092 -L 2181:127.0.0.1:2181 ${USER}@pg.internal.equalit.ie

echo "127.0.0.1 kafka-dev.localdomain">>/etc/hosts
echo "127.0.0.1  pg.internal.equalit.ie">>/etc/hosts

# Then start Baskerville
screen -d -m spark-submit --master spark://$SPARK_MASTER_HOST:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:${SPARK_VERSION},org.postgresql:postgresql:42.2.4 --jars $BASKERVILLE_ROOT/data/jars/postgresql-42.2.4.jar,$BASKERVILLE_ROOT/data/jars/spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar $BASKERVILLE_ROOT/src/baskerville/main.py kafka -c $BASKERVILLE_ROOT/conf/baskerville.conf
