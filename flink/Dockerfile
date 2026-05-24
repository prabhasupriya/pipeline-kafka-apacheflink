FROM flink:1.17.1-scala_2.12

USER root

# Install Python 3 and system requirements quickly
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev curl && \
    rm -rf /var/lib/apt/lists/*

RUN ln -sf /usr/bin/python3 /usr/bin/python

# Upgrade pip and install libraries along with kafka-python-ng for broadcasting lookups
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir apache-flink-libraries==1.17.1 && \
    pip3 install --no-cache-dir apache-flink==1.17.1 && \
    pip3 install --no-cache-dir kafka-python-ng==2.2.2

COPY lib/flink-sql-connector-kafka-3.0.1-1.17.jar /opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.17.jar

RUN chmod 644 /opt/flink/lib/flink-sql-connector-kafka-3.0.1-1.17.jar

WORKDIR /opt/flink/usrlib

COPY pipeline.py .

ENV FLINK_HOME=/opt/flink