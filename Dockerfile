FROM debian:jessie

# Base image for CommonSearch backend development
# Uses bits from:
# https://github.com/gettyimages/docker-spark/blob/master/Dockerfile
# https://github.com/docker-library/openjdk/blob/master/8-jre/Dockerfile

#
# httpredir.debian.org is often unreliable
# https://github.com/docker-library/buildpack-deps/issues/40
#

RUN echo \
   'deb ftp://ftp.us.debian.org/debian/ jessie main\n \
    deb ftp://ftp.us.debian.org/debian/ jessie-updates main\n \
    deb http://security.debian.org jessie/updates main\n' \
    > /etc/apt/sources.list

RUN echo 'deb http://ftp.us.debian.org/debian jessie-backports main' > /etc/apt/sources.list.d/jessie-backports.list

#
# General packages & dependencies
#

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/jre
ENV LANG C.UTF-8

RUN apt-get clean && apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    wget \
    git \
    gcc \
    build-essential \
    make \
    cmake \
    python \
    python-pip \
    python-dev \
    vim \
    zlib1g-dev \
    libbz2-dev \
    libsnappy-dev \
    libgflags-dev \
    libxml2-dev \
    libxslt-dev \
    libffi-dev \
    libtool \
    automake \
    strace \
    pkg-config \
    net-tools \
    unzip \
    dstat \
    openjdk-8-jre-headless \
    ca-certificates-java

# We could do this to save on image size but we're optimizing for developer experience instead
# && rm -rf /var/lib/apt/lists/*


#
# RocksDB
#

ENV ROCKSDB_VERSION 4.1
RUN wget https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz && \
  tar -zxvf v${ROCKSDB_VERSION}.tar.gz && cd rocksdb-${ROCKSDB_VERSION} && \
  PORTABLE=1 make shared_lib && make install && \
  cd .. && rm -rf rocksdb-${ROCKSDB_VERSION} v${ROCKSDB_VERSION}.tar.gz



#
# Install Gumbo
#

ENV GUMBO_VERSION 0.10.1
ENV LD_LIBRARY_PATH /usr/local/lib
RUN wget https://github.com/google/gumbo-parser/archive/v${GUMBO_VERSION}.tar.gz && \
  tar zxf v${GUMBO_VERSION}.tar.gz && cd gumbo-parser-${GUMBO_VERSION} && \
  ./autogen.sh && ./configure && make && make install && ldconfig && \
  cd .. && rm -rf gumbo-parser-${GUMBO_VERSION} v${GUMBO_VERSION}.tar.gz


#
# Install RE2
#
RUN mkdir -p /tmp/re2 && \
  curl -L 'https://github.com/google/re2/archive/636bc71728b7488c43f9441ecfc80bdb1905b3f0.tar.gz' -o /tmp/re2/re2.tar.gz && \
  cd /tmp/re2 && tar zxvf re2.tar.gz --strip-components=1 && \
  make && make install && \
  rm -rf /tmp/re2 && \
  ldconfig


#
# Install Protocol Buffers
#

# v3.0.0 doesn't build anymore - see https://github.com/google/protobuf/commit/1760feb621a913189b90fe8595fffb74bce84598
ENV PROTOBUF_VERSION a098e809336c5fbad7a8ff8f1210e5e0ac8d29b2
RUN curl -sL https://github.com/google/protobuf/archive/${PROTOBUF_VERSION}.tar.gz | tar zx && \
  cd protobuf-${PROTOBUF_VERSION} && \
  ./autogen.sh && ./configure && make && make install && ldconfig && \
  cd .. && rm -rf protobuf-${PROTOBUF_VERSION}


# Oracle JDK is recommended in some places versus Open JDK so it may be interesting to
# benchmark them or try Oracle JDK to single-out bugs in Open JDK. However it is closed-source
# so we can't use it.
# ENV JAVA_HOME /usr/jdk1.8.0_31
# ENV PATH $PATH:$JAVA_HOME/bin
# RUN curl -sL --retry 3 --insecure \
#   --header "Cookie: oraclelicense=accept-securebackup-cookie;" \
#   "http://download.oracle.com/otn-pub/java/jdk/8u31-b13/server-jre-8u31-linux-x64.tar.gz" \
#   | gunzip \
#   | tar x -C /usr/ \
#   && ln -s $JAVA_HOME /usr/java \
#   && rm -rf $JAVA_HOME/man



#
# Install Spark
#

# https://people.apache.org/~pwendell/spark-nightly/spark-branch-2.0-bin/spark-2.0.1-SNAPSHOT-2016_07_29_00_24-5cd79c3-bin/
ENV SPARK_VERSION 2.0.1-SNAPSHOT
ENV SPARK_HOME /usr/spark
ENV PATH $PATH:$SPARK_HOME/bin
ENV SPARK_CONF_DIR /cosr/back/spark/conf

# http://d3kbcqa49mib13.cloudfront.net/spark-$SPARK_VERSION-bin-without-hadoop.tgz
RUN curl -sL --retry 3 "https://s3.amazonaws.com/packages.commonsearch.org/spark/spark-2.0.1-SNAPSHOT-bin-hadoop2.7.tgz" \
  | tar xz -C /usr/ \
  && ls -la /usr/ \
  && ln -s /usr/spark-$SPARK_VERSION-bin-hadoop2.7 $SPARK_HOME

#
# Install Hadoop
#

ENV HADOOP_VERSION 2.7.2
ENV HADOOP_HOME /usr/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin
RUN curl -sL http://www.eu.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz | tar -xz -C /usr/
RUN cd /usr/ && ln -s ./hadoop-$HADOOP_VERSION hadoop


#
# Spark packages
#

RUN wget 'https://repo1.maven.org/maven2/org/apache/parquet/parquet-tools/1.8.1/parquet-tools-1.8.1.jar' -P /usr/lib/

# Download the current dependencies so we don't have to do it at first run
ADD spark/conf/spark-defaults.conf /tmp/spark.conf
RUN SPARK_DIST_CLASSPATH=$(hadoop classpath) spark-submit --properties-file /tmp/spark.conf /usr/spark/examples/src/main/python/pi.py && rm /tmp/spark.conf


#
# Install PyPy for performance testing
#

RUN curl -sL 'https://bitbucket.org/squeaky/portable-pypy/downloads/pypy-5.3.1-linux_x86_64-portable.tar.bz2' -o /pypy.tar.bz2 && \
  mkdir -p /opt/pypy/ && tar jxvf /pypy.tar.bz2 -C /opt/pypy/  --strip-components=1 && \
  rm /pypy.tar.bz2


#
# Python modules not on PyPI
#

# FAUP
RUN cd /tmp && \
    git clone https://github.com/stricaud/faup.git && \
    cd faup && \
    git checkout 07f9550fb288a94efccdaeeae66c34aafa91aded && \
    cd build && \
    cmake .. && make && \
    make install && \
    cd ../src/lib/bindings/python && \
    python setup.py install && \
    cd / && rm -rf /tmp/faup




#
# Python setup
#

ADD requirements.txt /requirements.txt

# Upgrade pip because debian has a really old version
RUN pip install --upgrade --ignore-installed pip

RUN pip install -r /requirements.txt


#
# Common Search setup
#

# Base directory
RUN mkdir -p /cosr/back

# Save the hash at the time the image was built
ADD .dockerhash /cosr/.back-dockerhash
