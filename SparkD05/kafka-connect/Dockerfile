FROM quay.io/strimzi/kafka:0.30.0-kafka-3.1.0
USER root:root
RUN mkdir -p /opt/kafka/plugins
RUN mkdir -p /opt/kafka/plugins/debezium
COPY ./mongo-kafka-connect-1.6.1-all.jar /opt/kafka/plugins
COPY ./debezium-connector-mysql/ /opt/kafka/plugins/debezium/
USER 1001
