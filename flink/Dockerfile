FROM flink:1.17.1-scala_2.12

USER root

# Install Python 3 and system requirements quickly
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

# Upgrade pip and install the pre-compiled library binaries FIRST
# This prevents pip from trying to compile the wheels from scratch!
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir apache-flink-libraries==1.17.1 && \
    pip3 install --no-cache-dir apache-flink==1.17.1

WORKDIR /opt/flink/usrlib