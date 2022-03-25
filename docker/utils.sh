#!/bin/bash

# ===============================[global variables]===============================
declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
declare -r BASE_VERSION="0.0.1"
declare -r BASE_REPO="ghcr.io/chia7712/kafka2delta/base"
declare -r BASE_IMAGE_NAME="$BASE_REPO:$BASE_VERSION"
declare -r BASE_DOCKERFILE=$DOCKER_FOLDER/base.dockerfile
declare -r ADDRESS=$([[ "$(which ipconfig)" != "" ]] && ipconfig getifaddr en0 || hostname -i)
declare -r CODE_FOLDER="$DOCKER_FOLDER/../code"
declare -r METADATA_FOLDER="$DOCKER_FOLDER/../metadata"
declare -r CSV_FOLDER="$DOCKER_FOLDER/../csv"
# ===============================[delta configs]===============================
declare -r DELTA_CONFIGS="--packages io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-azure:3.2.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
                          --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
                          --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
                          --conf spark.sql.streaming.kafka.useDeprecatedOffsetFetching=false \
                          --conf spark.databricks.delta.merge.repartitionBeforeWrite.enabled=true"
# ===================================[check functions]===================================
function requireFolder() {
  local path=$1
  if [[ ! -d "$path" ]]; then
    echo "$1 is not folder"
    exit 2
  fi
}

function requireNonEmpty() {
  local var=$1
  local message=$2
  if [[ "$var" == "" ]]; then
    echo "$message"
    exit 2
  fi
}

function checkGen2Args() {
  local gen2_account=$1
  local gen2_container=$2
  local gen2_key=$3
  local pass="false"
  if [[ "$gen2_account" != "" ]] && [[ "$gen2_container" != "" ]] && [[ "$gen2_key" != "" ]]; then
    pass="true"
  fi
  if [[ "$gen2_account" == "" ]] && [[ "$gen2_container" == "" ]] && [[ "$gen2_key" == "" ]]; then
    pass="true"
  fi

  if [[ "$pass" != "true" ]]; then
    echo "inconsistent configs for gen2"
    exit 2
  fi
}

function checkOs() {
  if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "This script requires to run container with \"--network host\", but the feature is unsupported by Mac OS"
    exit 2
  fi
}
# ===================================[docker functions]===================================
function downloadBaseImage() {
  if [[ "$(docker images -q $BASE_IMAGE_NAME 2>/dev/null)" == "" ]]; then
    docker pull $BASE_IMAGE_NAME
    if [[ "$?" != "0" ]]; then
      echo "$BASE_IMAGE_NAME is not in github package. Please execute $DOCKER_FOLDER/build_base_image.sh first"
      exit 2
    fi
  fi
}

function generateDockerfileForCode() {
  local image_name=$1
  local dockerfile=$2
  local master=$3
  local cache="$DOCKER_FOLDER/context"
  if [[ -d "$cache" ]]; then
    rm -rf "$cache"
  fi
  mkdir -p "$cache"
  cp -r "$CODE_FOLDER" "$cache/code"
  cp -r "$METADATA_FOLDER" "$cache/metadata"
  echo "# this dockerfile is generated dynamically
FROM $BASE_IMAGE_NAME

# copy py files to docker image
COPY --chown=astraea context/code $CODE_FOLDER_IN_CONTAINER
COPY --chown=astraea context/metadata $METADATA_FOLDER_IN_CONTAINER

WORKDIR /opt/spark
" >"$dockerfile"

  if [[ "$master" == "k8s"* ]]; then
    if [[ ! -f "$HOME/.kube/config" ]]; then
      echo "Kubernetes config file is required. Please make sure $HOME/.kube/config existent in this host"
      exit 2
    fi

    # install k8s config if needs
    cp "$HOME/.kube/config" "$DOCKER_FOLDER/context/config"

    if [[ ! -f "$DOCKER_FOLDER/context/config" ]]; then
      echo "failed to copy k8s config to $DOCKER_FOLDER/context/config"
      exit 2
    fi

    echo "# this dockerfile is generated dynamically
RUN mkdir -p /home/astraea/.kube
COPY --chown=astraea context/config /home/astraea/.kube/
ENV SPARK_EXTRA_CLASSPATH=\"/home/astraea/.ivy2/jars/*\"
ENTRYPOINT [\"/opt/spark/kubernetes/dockerfiles/spark/entrypoint.sh\"]

WORKDIR /opt/spark
" >>"$dockerfile"
  fi
}
# ===================================[k8s functions]===================================
function generateK8sConfigs() {
  local namespace=$1
  local account=$2
  local image=$3
  echo "--conf spark.kubernetes.namespace=$namespace \
        --conf spark.kubernetes.authenticate.driver.serviceAccountName=$account \
        --conf spark.kubernetes.container.image=$image \
        --conf spark.kubernetes.container.image.pullPolicy=Always"
}
# ===================================[gen2 functions]===================================
function generateGen2Configs() {
  local gen2_account=$1
  local gen2_container=$2
  local gen2_key=$3
  echo "--conf spark.hadoop.fs.azure.account.auth.type.${gen2_account}.dfs.core.windows.net=SharedKey \
        --conf spark.hadoop.fs.azure.account.key.${gen2_account}.dfs.core.windows.net=$gen2_key"
}

function generateGen2Path() {
  local gen2_account=$1
  local gen2_container=$2
  local path=$3
  echo "abfs://${gen2_container}@${gen2_account}.dfs.core.windows.net/${path}"
}
