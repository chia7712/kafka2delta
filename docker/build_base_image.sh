#!/bin/bash

# ===============================[read utils]===============================
source "$(dirname "$0")"/utils.sh
# ===============================[global variables]===============================
declare -r SPARK_VERSION="3.1.2"
declare -r KAFKA_PYTHON_VERSION="1.7.0"
# 3.2.2 fixes the NEP (see https://issues.apache.org/jira/browse/HADOOP-16410)
declare -r HADOOP_VERSION="3.2.2"
declare -r DELTA_VERSION="1.0.0"
declare -r IVY_VERSION="2.5.0"
declare -r IMAGE_NAME=$BASE_IMAGE_NAME
declare -r DOCKERFILE=$BASE_DOCKERFILE
# ===================================[functions]===================================
function generateDockerfile() {
  echo "# this dockerfile is generated dynamically
FROM ubuntu AS build

RUN apt-get update && apt-get install -y wget zip openjdk-11-jre

# download ivy
WORKDIR /tmp
RUN wget https://dlcdn.apache.org//ant/ivy/2.5.0/apache-ivy-2.5.0-bin.zip
RUN unzip apache-ivy-${IVY_VERSION}-bin.zip
WORKDIR /tmp/apache-ivy-${IVY_VERSION}
RUN java -jar ./ivy-${IVY_VERSION}.jar -dependency io.delta delta-core_2.12 $DELTA_VERSION
RUN java -jar ./ivy-${IVY_VERSION}.jar -dependency org.apache.spark spark-sql-kafka-0-10_2.12 $SPARK_VERSION
RUN java -jar ./ivy-${IVY_VERSION}.jar -dependency org.apache.spark spark-token-provider-kafka-0-10_2.12 $SPARK_VERSION
RUN java -jar ./ivy-${IVY_VERSION}.jar -dependency org.apache.hadoop hadoop-azure $HADOOP_VERSION

FROM ghcr.io/skiptests/astraea/spark:$SPARK_VERSION

# install python dependencies
RUN pip3 install confluent-kafka==$KAFKA_PYTHON_VERSION delta-spark==$DELTA_VERSION pyspark==$SPARK_VERSION

# copy jars from ivy
COPY --from=build /root/.ivy2 /home/astraea/.ivy2
USER root
RUN chown -R astraea:astraea /home/astraea/.ivy2
USER astraea

# collect all jars
RUN mkdir -p /home/astraea/.ivy2/jars/
RUN cp /home/astraea/.ivy2/cache/*/*/jars/*.jar /home/astraea/.ivy2/jars/

# move back to spark home
WORKDIR /opt/spark
" >"$DOCKERFILE"
}

function buildBaseImageIfNeed() {
  if [[ "$(docker images -q $IMAGE_NAME 2>/dev/null)" == "" ]]; then
    docker build --no-cache -t "$IMAGE_NAME" -f "$DOCKERFILE" "$DOCKER_FOLDER"
    docker push $IMAGE_NAME
  else
    echo "$IMAGE_NAME is existent in local"
  fi
}
# ===================================[main]===================================
generateDockerfile
buildBaseImageIfNeed