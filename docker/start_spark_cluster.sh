#!/bin/bash

# ===============================[read utils]===============================
source "$(dirname "$0")"/utils.sh
# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_spark_cluster.sh"
  echo "Arguments: "
}

function downloadBaseImage() {
  docker pull $BASE_IMAGE_NAME
  if [[ "$(docker images -q $BASE_IMAGE_NAME 2>/dev/null)" == "" ]]; then
    echo "$BASE_IMAGE_NAME is not in github package. Please execute $DOCKER_FOLDER/build_base_image.sh first"
    exit 2
  fi
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

downloadBaseImage

if [[ -f "$DOCKER_FOLDER/start_spark.sh" ]]; then
  rm -f "$DOCKER_FOLDER/start_spark.sh"
fi

curl -H "Cache-Control: no-cache" https://raw.githubusercontent.com/skiptests/astraea/main/docker/start_spark.sh -o "$DOCKER_FOLDER/start_spark.sh"

# use custom spark image
export REPO=$BASE_REPO
export VERSION=$BASE_VERSION

# master
export SPARK_PORT=$(($(($RANDOM % 10000)) + 10000))
export SPARK_UI_PORT=$(($(($RANDOM % 10000)) + 10000))
bash $DOCKER_FOLDER/start_spark.sh

# worker
master_url="spark://$ADDRESS:$SPARK_PORT"
export SPARK_PORT=$(($(($RANDOM % 10000)) + 10000))
export SPARK_UI_PORT=$(($(($RANDOM % 10000)) + 10000))
bash $DOCKER_FOLDER/start_spark.sh "$master_url"

