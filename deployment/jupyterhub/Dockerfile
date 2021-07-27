FROM bitnami/jupyterhub:1.4.2-debian-10-r0

# install Baskerville

USER root

RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git

RUN pip install --no-cache-dir --upgrade pip
WORKDIR /usr/local
RUN git clone --single-branch --branch categorical_features https://github.com/equalitie/spark-iforest.git
WORKDIR /usr/local/spark-iforest/python
RUN python setup.py sdist
RUN pip install dist/pyspark-iforest-2.4.0.99.tar.gz
WORKDIR /usr/local
RUN git clone https://github.com/equalitie/esretriever.git

WORKDIR esretriever
RUN rm requirements.txt && \
    echo 'pyspark==2.4.6' >> requirements.txt && \
    echo 'python-dateutil==2.7.3' >> requirements.txt && \
    echo 'requests==2.20.0' >> requirements.txt

RUN pip install .
WORKDIR /usr/local
RUN git clone --single-branch --branch develop https://github.com/equalitie/baskerville.git
WORKDIR /usr/local/baskerville
RUN pip install -e .
RUN mkdir -p src/baskerville/logs

ENV BASKERVILLE_ROOT=/usr/local/baskerville
ENV PYSPARK_PYTHON=python3.7