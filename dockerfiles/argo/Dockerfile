FROM openjdk:8
FROM python:3.6

RUN mkdir /usr/lib/spark
COPY spark-2.4.7-bin-hadoop2.7.tgz /usr/lib/spark

RUN apt-get update && apt-get install -y \
 wget \
 default-jdk \
 scala && cd /usr/lib/spark && \
 tar -xvf spark-2.4.7-bin-hadoop2.7.tgz

ENV SPARK_HOME /usr/lib/spark/spark-2.4.7-bin-hadoop2.7
ENV PYSPARK_PYTHON python3.6
ENV PATH $PATH:$SPARK_HOME/bin

RUN apt-get update
RUN apt-get install -y apt-transport-https ca-certificates curl gnupg-agent software-properties-common
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -
RUN add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
RUN apt-get update
RUN apt-get install -y docker-ce docker-ce-cli containerd.io

sudo apt-get update && sudo apt-get install -y apt-transport-https gnupg2 curl
curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
echo "deb https://apt.kubernetes.io/ kubernetes-xenial main" | sudo tee -a /etc/apt/sources.list.d/kubernetes.list
sudo apt-get update
sudo apt-get install -y kubectl




