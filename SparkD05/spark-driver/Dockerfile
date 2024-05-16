# Stage 1: Build the Spark application using SBT
FROM openjdk:8-slim as build
ENV SBT_VERSION 1.6.2
WORKDIR /app

# Install SBT
RUN apt-get update && apt-get install -y curl
RUN curl -fsL https://github.com/sbt/sbt/releases/download/v$SBT_VERSION/sbt-$SBT_VERSION.tgz | tar xfz - -C /usr/local
ENV PATH /usr/local/sbt/bin:${PATH}

# Copy project files
COPY build.sbt /app/
COPY project /app/project
COPY src /app/src

# Build the projects
RUN sbt clean package
# Stage 2: Build the final image
FROM ghcr.io/googlecloudplatform/spark-operator:v1beta2-1.3.8-3.1.1 AS operator
#FROM ${SPARK_IMAGE} AS operator
#ARG SPARK_IMAGE
USER root

# Minio-jars
COPY spark-driver/hadoop-aws-2.7.4.jar /opt/spark/jars
COPY spark-driver/aws-java-sdk-1.7.4.jar /opt/spark/jars

##### Kafka Structured Streaming jars
COPY spark-driver/commons-pool2-2.6.2.jar /opt/spark/jars
COPY spark-driver/kafka-clients-2.6.0.jar /opt/spark/jars
COPY spark-driver/spark-sql-kafka-0-10_2.12-3.1.1.jar /opt/spark/jars
COPY spark-driver/spark-token-provider-kafka-0-10_2.12-3.1.1.jar /opt/spark/jars
COPY spark-driver/scala-reflect-2.12.8.jar /opt/spark/jars
COPY spark-driver/scala-library-2.12.8.jar /opt/spark/jars
COPY spark-driver/spark-core_2.12-3.0.1.jar /opt/spark/jars

# Copy the built JAR file from the build stage
COPY --from=build /app/target/scala-*/*.jar /opt/spark/app-jars/SparkDO5.jar

ENTRYPOINT ["/opt/entrypoint.sh"]
