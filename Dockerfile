FROM debian:jessie

# Base image for CommonSearch backend development
# Uses bits from:
# https://github.com/gettyimages/docker-spark/blob/master/Dockerfile
# https://github.com/docker-library/openjdk/blob/master/8-jre/Dockerfile

#
# General packages & dependencies
#

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/jre
ENV LANG C.UTF-8
ENV JAVA_VERSION 8u91
ENV JAVA_DEBIAN_VERSION 8u91-b14-1~bpo8+1
# see https://bugs.debian.org/775775
# and https://github.com/docker-library/java/issues/19#issuecomment-70546872
ENV CA_CERTIFICATES_JAVA_VERSION 20140324

RUN echo 'deb http://httpredir.debian.org/debian jessie-backports main' > /etc/apt/sources.list.d/jessie-backports.list

RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    wget \
    git \
    gcc \
    build-essential \
    make \
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
    openjdk-8-jre-headless="$JAVA_DEBIAN_VERSION" \
    ca-certificates-java="$CA_CERTIFICATES_JAVA_VERSION"

# We could do this to save on image size but we're optimizing for developer experience instead
# && rm -rf /var/lib/apt/lists/*

# see CA_CERTIFICATES_JAVA_VERSION notes above
RUN /var/lib/dpkg/info/ca-certificates-java.postinst configure




#
# RocksDB
#

ENV ROCKSDB_VERSION 4.1
RUN wget https://github.com/facebook/rocksdb/archive/v${ROCKSDB_VERSION}.tar.gz && \
  tar -zxvf v${ROCKSDB_VERSION}.tar.gz && cd rocksdb-${ROCKSDB_VERSION} && \
  make shared_lib && make install && \
  cd .. && rm -rf rocksdb-${ROCKSDB_VERSION} v${ROCKSDB_VERSION}.tar.gz



#
# Install PyPy for performance testing
#

RUN curl -L 'https://bitbucket.org/squeaky/portable-pypy/downloads/pypy-5.3.1-linux_x86_64-portable.tar.bz2' -o /pypy.tar.bz2 && \
  mkdir -p /opt/pypy/ && tar jxvf /pypy.tar.bz2 -C /opt/pypy/  --strip-components=1 && \
  rm /pypy.tar.bz2



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
# Install Spark
#

ENV SPARK_VERSION 1.6.2
ENV HADOOP_VERSION 2.6
ENV SPARK_PACKAGE spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION
ENV SPARK_HOME /usr/$SPARK_PACKAGE
ENV PATH $PATH:$SPARK_HOME/bin

# Despite the cryptic URL, this is Apache's official repository
RUN curl -sL --retry 3 \
  "http://d3kbcqa49mib13.cloudfront.net/$SPARK_PACKAGE.tgz" \
  | gunzip \
  | tar x -C /usr/ \
  && ln -s $SPARK_HOME /usr/spark


#
# Install Protocol Buffers
#

ENV PROTOBUF_VERSION 3.0.0-beta-3.1
RUN wget https://codeload.github.com/google/protobuf/tar.gz/v${PROTOBUF_VERSION} && \
  tar zxf v${PROTOBUF_VERSION} && cd protobuf-${PROTOBUF_VERSION} && \
  ./autogen.sh && ./configure && make && make install && ldconfig && \
  cd .. && rm -rf protobuf-${PROTOBUF_VERSION} v${PROTOBUF_VERSION}




#
# Python setup
#

ADD requirements.txt /requirements.txt

# Upgrade pip because debian has a really old version
RUN pip install --upgrade --ignore-installed pip

RUN pip install -r /requirements.txt

# RUN ln -s /usr/local/lib/libgumbo.so /usr/local/lib/python2.7/dist-packages/gumbo/libgumbo.so

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
# Common Search setup
#

# Base directory
RUN mkdir -p /cosr/back
