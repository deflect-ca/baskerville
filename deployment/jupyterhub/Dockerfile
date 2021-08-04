FROM ubuntu:18.04

ARG SPARK_VER=2.4.6

WORKDIR /root

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y && \
    apt-get clean && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Install a base time build environment
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
	apt-utils \
	build-essential \
	curl \
	git \
	libpng-dev \
	libfreetype6-dev \
	pkg-config \
	python3.7 \
	python3.7-dev \
	python3.7-distutils \
	software-properties-common \
	sudo \
	tar \
	unzip \
	wget \
	vim && \
    apt-get clean && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Get the latest pip3
RUN wget -q https://bootstrap.pypa.io/get-pip.py && python3.7 get-pip.py && rm -f get-pip.py

# Install python3 infrastructure
COPY requirements.txt .
RUN pip3 --no-cache-dir install -r requirements.txt && rm -f requirements.txt

# Install Spark dependencies
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
	openjdk-8-jre \
	scala && \
	apt-get clean && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# Install Spark
RUN wget -q -O spark.tgz http://mirrors.myaegean.gr/apache/spark/spark-${SPARK_VER}/spark-${SPARK_VER}-bin-hadoop2.7.tgz && \
	mkdir -p /usr/local && tar -xf spark.tgz -C /usr/local && \
	mv /usr/local/spark*${SPARK_VER}* /usr/local/spark && \
    	pip3 install --upgrade pyspark

# Clean up
RUN rm -rf /root/* && chmod 755 /root

RUN jupyter notebook --generate-config
RUN sed -i -E 's,^#\s*c.NotebookApp.terminado_settings.*,c.NotebookApp.terminado_settings = {"shell_command" : ["\/bin\/bash"]},g' ~/.jupyter/jupyter_notebook_config.py

# Setup password authentication so we don't have to remember tokens (password: "jupyter")
RUN echo "{ \"NotebookApp\": { \"password\": \"sha1:ad16e87de314:a02efac10ccd7ead24e845f438b2b87fe8bc2d0f\" } }" >> ~/.jupyter/jupyter_notebook_config.json
RUN mkdir -p ~/.ipython/profile_default
RUN echo "c.TerminalInteractiveShell.editing_mode = 'vi'" >> ~/.ipython/profile_default/ipython_config.py

# Required spark configuration for local user access
ENV SPARK_HOME=/usr/local/spark
ENV PYSPARK_PYTHON=python3.7
ENV PYSPARK_DRIVER_PYTHON=python3.7
ENV PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.7-src.zip
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/spark/bin

EXPOSE 4040 4041 8080 8888

# install Baskerville
#
USER root

RUN pip3 install -U pip3 setuptools

WORKDIR /usr/local
RUN git clone --single-branch --branch categorical_features https://github.com/equalitie/spark-iforest.git
WORKDIR /usr/local/spark-iforest/python
RUN python3 setup.py sdist
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
RUN git clone --single-branch --branch k8s_doc https://github.com/equalitie/baskerville.git
WORKDIR /usr/local/baskerville
RUN apt-get -y install gcc

RUN pip install -e .
RUN mkdir -p src/baskerville/logs

ENV BASKERVILLE_ROOT=/usr/local/baskerville
# ENV PYSPARK_PYTHON=python3.7

COPY ./conf /usr/local/baskerville/conf

CMD ["notebook", "--no-browser", "--allow-root", "--ip=0.0.0.0", "--port=8888"]
ENTRYPOINT ["/usr/local/bin/jupyter"]

