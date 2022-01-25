#!/bin/bash

# ===============================[global variables]===============================

declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
declare -r ADDRESS=$([[ "$(which ipconfig)" != "" ]] && ipconfig getifaddr en0 || hostname -i)
declare -r BASE_IMAGE_NAME="ghcr.io/chia7712/kafka2delta/spark:3.1.2"
declare -r BASE_DOCKERFILE=$DOCKER_FOLDER/base.dockerfile

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_spark_cluster.sh"
  echo "Arguments: "
}

# ===================================[main]===================================

while [[ $# -gt 0 ]]; do
  case $1 in
  --help)
    showHelp
    exit 0
    ;;
  *)
    echo "Unknown option $1"
    exit 1
    ;;
  esac
done

# build base image first
if [[ "$(docker images -q $BASE_IMAGE_NAME 2>/dev/null)" == "" ]]; then
  docker build --no-cache -t "$BASE_IMAGE_NAME" -f "$BASE_DOCKERFILE" "$DOCKER_FOLDER"
fi

if [[ -f "$DOCKER_FOLDER/start_spark.sh" ]]; then
  rm -f "$DOCKER_FOLDER/start_spark.sh"
fi

curl -H "Cache-Control: no-cache" https://raw.githubusercontent.com/skiptests/astraea/main/docker/start_spark.sh -o "$DOCKER_FOLDER/start_spark.sh"

# use custom spark image
export REPO="ghcr.io/chia7712/kafka2delta/spark"
export VERSION="3.1.2"

# master
export SPARK_PORT=$(($(($RANDOM % 10000)) + 10000))
export SPARK_UI_PORT=$(($(($RANDOM % 10000)) + 10000))
bash $DOCKER_FOLDER/start_spark.sh

# worker
master_url="spark://$ADDRESS:$SPARK_PORT"
export SPARK_PORT=$(($(($RANDOM % 10000)) + 10000))
export SPARK_UI_PORT=$(($(($RANDOM % 10000)) + 10000))
bash $DOCKER_FOLDER/start_spark.sh "$master_url"

