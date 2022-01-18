#!/bin/bash

# ===============================[global variables]===============================

declare -r USER=astraea
declare -r IMAGE_NAME="ghcr.io/skiptests/astraea/spark:3.1.2"
declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
declare -r ADDRESS=$([[ "$(which ipconfig)" != "" ]] && ipconfig getifaddr en0 || hostname -i)

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] submit_write_kafka.sh"
  echo "Arguments: "
  echo "    --brokers     kafka bootstrap servers"
  echo "    --metadata    metadata files location"
  echo "    --csv         csv files location"
  echo "    --main        main python file"
  echo "    --utils       utils python file"
}

function requireFolder() {
  local path=$1
  if [[ ! -d "$path" ]]; then
    echo "$1 is not folder"
    exit 2
  fi
}

function requirePythonFile() {
  local path=$1
  if [[ ! -f "$path" ]]; then
    echo "$1 is not python file"
    exit 2
  fi
}

function checkImage() {
  if [[ "$(docker images -q $IMAGE_NAME 2>/dev/null)" == "" ]]; then
    echo "$IMAGE_NAME is nonexistent"
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

# ===================================[main]===================================

brokers=""
csv_folder="$DOCKER_FOLDER/../csv"
metadata_folder="$DOCKER_FOLDER/../metadata"
main_python="$DOCKER_FOLDER/../write_kafka.py"
utils_python="$DOCKER_FOLDER/../utils.py"
while [[ $# -gt 0 ]]; do
  case $1 in
  --main)
    main_python="$2"
    shift
    shift
    ;;
  --utils)
    utils_python="$2"
    shift
    shift
    ;;
  --metadata)
    metadata_folder="$2"
    shift
    shift
    ;;
  --csv)
    csv_folder="$2"
    shift
    shift
    ;;
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

checkImage
requireFolder "$metadata_folder"
requireFolder "$csv_folder"
requirePythonFile "$main_python"
requirePythonFile "$utils_python"
requireNonEmpty "$brokers" "--brokers is required"

for meta_file in "$metadata_folder"/*.xml; do
  filename=$(basename -- "$meta_file")
  container_name="${filename%.*}-kafka"
  if [[ "$(docker ps --format={{.Names}} | grep -w $container_name)" != "" ]]; then
    echo "container: $container_name is already running"
  else
    port="$(($(($RANDOM % 10000)) + 10000))"
    docker run -d \
      --name $container_name \
      -p $port:4040 \
      -v "$meta_file":/tmp/schema.xml:ro \
      -v "$main_python":/tmp/main.py:ro \
      -v "$utils_python":/tmp/utils.py:ro \
      -v "$csv_folder":/tmp/data:ro \
      $IMAGE_NAME \
      ./bin/spark-submit \
      --name $container_name \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
      --py-files /tmp/utils.py \
      --master "local[*]" \
      /tmp/main.py \
      --bootstrap_servers $brokers \
      --schema_file /tmp/schema.xml \
      --csv_folder /tmp/data >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      echo "failed to submit job for schema: $filename"
    else
      echo "check UI: http://$ADDRESS:$port for schema: $filename"
    fi
  fi
done
