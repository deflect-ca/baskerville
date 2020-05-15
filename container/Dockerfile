FROM openjdk:8
FROM python:3.6

ARG GITLAB_USER
ARG GITLAB_KEY
# necessary:
ARG DB_USER
ARG DB_PASSWORD
ARG DB_HOST
ARG DB_PORT
ARG BASKERVILLE_BRANCH
# optional - depending on the pipeline to be ran:
ARG ELK_USER
ARG ELK_PASSWORD
ARG ELK_HOST
ARG KAFKA_HOST

# Get jdk8 from previous stage https://docs.docker.com/develop/develop-images/multistage-build/
COPY --from=openjdk:8 /usr/local/openjdk-8 /usr/local/openjdk-8

# Set java path
ENV JAVA_HOME /usr/local/openjdk-8
ENV PATH $PATH:$JAVA_HOME/bin

# To solve add-apt-repository : command not found
RUN apt-get update && apt-get -y install software-properties-common

# Baskerville
# Currently esretriever is downloaded and installed separately before Baskerville
# Also, pip >= 19 breaks the --process-dependency-links functionality
RUN mkdir app \
    && cd app \
    && pip install --upgrade pip==18.0 \
    && git clone https://github.com/titicaca/spark-iforest.git \
    && cd spark-iforest/python \
    && pip install . \
    && cd ../../ \
    && git clone https://$GITLAB_USER:$GITLAB_KEY@gitlab.internal.equalit.ie/mkaranasou/esretriever.git \
    && cd esretriever \
    && pip install . \
    && cd ../ \
    && git clone --branch $BASKERVILLE_BRANCH https://$GITLAB_USER:$GITLAB_KEY@gitlab.internal.equalit.ie/deflect/baskerville.git \
    && cd baskerville \
    && pip install .[test] \
    && mkdir src/baskerville/logs \
    && pip install --upgrade pip \
    && cd src/baskerville \
    && apt-get -y autoremove

ENV BASKERVILLE_ROOT /app/baskerville
COPY ./conf/test_baskerville_conf.yaml /app/baskerville/conf/baskerville.yaml

RUN ["chmod", "u+rwx", "app/baskerville/src/baskerville/main.py"]

WORKDIR /app/baskerville/src/baskerville

# metrics port, spark ui ports
EXPOSE 8998 4040 4041