#!/bin/bash

# ===============================[read utils]===============================
source "$(dirname "$0")"/utils.sh
# ===============================[global variables]===============================
declare -r IMAGE_NAME="ghcr.io/chia7712/kafka2delta/c2k:$BASE_VERSION"
declare -r DOCKERFILE=$DOCKER_FOLDER/c2k.dockerfile
# ===============================[variables in container]===============================
declare -r CODE_FOLDER_IN_CONTAINER="/tmp/code"
declare -r MAIN_PATH_IN_CONTAINER="$CODE_FOLDER_IN_CONTAINER/write_kafka.py"
declare -r METADATA_FOLDER_IN_CONTAINER="/tmp/metadata"
declare -r CSV_FOLDER_IN_CONTAINER="/tmp/input"
# ===================================[functions]===================================
function showHelp() {
  echo "Usage: [ENV] submit_write_kafka.sh"
  echo "Arguments: "
  echo "    --brokers     kafka bootstrap servers"
}

function buildImageIfNeed() {
  generateDockerfileForCode "$IMAGE_NAME" "$DOCKERFILE"
  docker build --no-cache -t "$IMAGE_NAME" -f "$DOCKERFILE" "$DOCKER_FOLDER"
  if [[ "$?" != "0" ]]; then
    exit 2
  fi
}
# ===================================[main]===================================

brokers=""
while [[ $# -gt 0 ]]; do
  case $1 in
  --brokers)
    brokers="$2"
    shift
    shift
    ;;
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

requireFolder "$CODE_FOLDER"
requireFolder "$METADATA_FOLDER"
requireFolder "$CSV_FOLDER"
requireNonEmpty "$brokers" "--brokers is required"
downloadBaseImage
buildImageIfNeed

for meta_file in "$METADATA_FOLDER"/*.xml; do
  meta_name=$(basename -- "$meta_file")
  container_name="${meta_name%.*}-kafka"
  if [[ "$(docker ps --format={{.Names}} | grep -w $container_name)" != "" ]]; then
    echo "container: $container_name is already running"
    continue
  fi
  port="$(($(($RANDOM % 10000)) + 10000))"
  # noted that the permission of csv folder is RW since we will move completed csv files to archive folder
  docker run -d \
    --name "$container_name" \
    -p $port:4040 \
    -v "$CSV_FOLDER":"$CSV_FOLDER_IN_CONTAINER" \
    $IMAGE_NAME \
    ./bin/spark-submit \
    --name "$container_name" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
    --master "local[*]" \
    $MAIN_PATH_IN_CONTAINER \
    --bootstrap_servers "$brokers" \
    --schema_file "$METADATA_FOLDER_IN_CONTAINER/$meta_name" \
    --csv_folder $CSV_FOLDER_IN_CONTAINER >/dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo "failed to submit job for schema: $meta_name"
  else
    echo "check UI: http://$ADDRESS:$port for schema: $meta_name"
  fi
done
