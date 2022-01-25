#!/bin/bash

# ===============================[global variables]===============================

declare -r BASE_IMAGE_NAME="ghcr.io/chia7712/kafka2delta/spark:3.1.2"
declare -r IMAGE_NAME="ghcr.io/chia7712/kafka2delta/k2d:0.0.1"
declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
declare -r BASE_DOCKERFILE=$DOCKER_FOLDER/base.dockerfile
declare -r DOCKERFILE=$DOCKER_FOLDER/k2d.dockerfile
declare -r ADDRESS=$([[ "$(which ipconfig)" != "" ]] && ipconfig getifaddr en0 || hostname -i)
declare -r CODE_FOLDER="$DOCKER_FOLDER/../code"
declare -r METADATA_FOLDER="$DOCKER_FOLDER/../metadata"
# ===============================[variables in container]===============================
declare -r CODE_FOLDER_IN_CONTAINER="/tmp/code"
declare -r MAIN_PATH_IN_CONTAINER="$CODE_FOLDER_IN_CONTAINER/write_delta.py"
declare -r METADATA_FOLDER_IN_CONTAINER="/tmp/metadata"

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] submit_write_delta.sh"
  echo "Arguments: "
  echo "    --account     gen2 account"
  echo "    --container   gen2 container"
  echo "    --key         gen2 share key"
  echo "    --path        gen2 path"
  echo "    --brokers     kafka bootstrap servers"
  echo "    --mode        spark submit mode"
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

function requireNonEmpty() {
  local var=$1
  local message=$2
  if [[ "$var" == "" ]]; then
    echo "$message"
    exit 2
  fi
}

function checkOs() {
  if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "This script requires to run container with \"--network host\", but the feature is unsupported by Mac OS"
    exit 2
  fi
}

function buildBaseImageIfNeed() {
  # the spark image from astraea does not have python deps and delta deps, so we replace it by our custom image
  if [[ "$(docker images -q $BASE_IMAGE_NAME 2>/dev/null)" == "" ]]; then
    docker build --no-cache -t "$BASE_IMAGE_NAME" -f "$BASE_DOCKERFILE" "$DOCKER_FOLDER"
  fi
}

function buildImageIfNeed() {
  if [[ "$(docker images -q $IMAGE_NAME 2>/dev/null)" != "" ]]; then
    docker rmi $IMAGE_NAME
    if [[ "$?" != "0" ]]; then
      exit 2
    fi
  fi
  local cache="$DOCKER_FOLDER/context/k2d"
  if [[ -d "$cache" ]]; then
    rm -rf "$cache"
  fi
  mkdir -p "$cache"
  cp -r "$CODE_FOLDER" "$cache/code"
  cp -r "$METADATA_FOLDER" "$cache/metadata"
  echo "# this dockerfile is generated dynamically
FROM $BASE_IMAGE_NAME

# copy py files to docker image
COPY --chown=astraea context/k2d/code $CODE_FOLDER_IN_CONTAINER
COPY --chown=astraea context/k2d/metadata $METADATA_FOLDER_IN_CONTAINER

WORKDIR /opt/spark
" >"$DOCKERFILE"

  docker build --no-cache -t "$IMAGE_NAME" -f "$DOCKERFILE" "$DOCKER_FOLDER"
  if [[ "$?" != "0" ]]; then
    exit 2
  fi
}

# ===================================[main]===================================

gen2_account=""
gen2_container=""
gen2_key=""
path="/tmp/delta-$(($(($RANDOM % 10000)) + 10000))"
mode="local[*]"
brokers=""
use_merge="false"
while [[ $# -gt 0 ]]; do
  case $1 in
  --account)
    gen2_account="$2"
    shift
    shift
    ;;
  --container)
    gen2_container="$2"
    shift
    shift
    ;;
  --key)
    gen2_key="$2"
    shift
    shift
    ;;
  --path)
    path="$2"
    shift
    shift
    ;;
  --brokers)
    brokers="$2"
    shift
    shift
    ;;
  --mode)
    mode="$2"
    shift
    shift
    ;;
  --merge)
    use_merge="$2"
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
requireNonEmpty "$brokers" "--brokers is required"
buildBaseImageIfNeed
buildImageIfNeed

# generate different mount command and gen2 command for different output (local or gen2)
gen2_command=""
output=""
mount_output=""
if [[ "$gen2_account" != "" ]] && [[ "$gen2_container" != "" ]] && [[ "$gen2_key" != "" ]]; then
  gen2_command="--conf spark.hadoop.fs.azure.account.auth.type.${gen2_account}.dfs.core.windows.net=SharedKey \
  --conf spark.hadoop.fs.azure.account.key.${gen2_account}.dfs.core.windows.net=$gen2_key"
  output="abfs://${gen2_container}@${gen2_account}.dfs.core.windows.net/${path}"
  echo "write data to azure"
else
  mkdir -p $path
  output="/tmp/output"
  mount_output="-v $path:/tmp/output"
  echo "write data to local: $path"
fi

# the spark image from astraea does not have python deps and delta deps, so we replace it by our custom image
if [[ "$(docker images -q $BASE_IMAGE_NAME 2>/dev/null)" == "" ]]; then
  docker build --no-cache -t "$BASE_IMAGE_NAME" -f "$BASE_DOCKERFILE" "$DOCKER_FOLDER"
fi

for meta_file in "$METADATA_FOLDER"/*.xml; do
  meta_name=$(basename -- "$meta_file")
  container_name="${meta_name%.*}-delta"
  if [[ "$(docker ps --format={{.Names}} | grep -w $container_name)" != "" ]]; then
    echo "container: $container_name is already running"
  else
    port="$(($(($RANDOM % 10000)) + 10000))"
    network="-p $port:4040"
    if [[ ! "$mode" == *"local"* ]]; then
      checkOs
      network="--network host"
    fi

    docker run -d \
      --name $container_name \
      $network \
      $mount_output \
      $IMAGE_NAME \
      ./bin/spark-submit \
      --name $container_name \
      --packages io.delta:delta-core_2.12:1.0.0,org.apache.hadoop:hadoop-azure:3.2.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
      $gen2_command \
      --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
      --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
      --conf spark.sql.streaming.kafka.useDeprecatedOffsetFetching=false \
      --conf spark.cores.max=12 \
      --conf spark.driver.cores=2 \
      --conf spark.driver.memory=2g \
      --conf spark.executor.cores=4 \
      --conf spark.executor.memory=10g \
      --conf spark.executor.instances=2 \
      --conf spark.databricks.delta.merge.repartitionBeforeWrite.enabled=true \
      --master $mode \
      $MAIN_PATH_IN_CONTAINER \
      --output $output \
      --bootstrap_servers $brokers \
      --schema_file $METADATA_FOLDER_IN_CONTAINER/$meta_name \
      --merge $use_merge >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      echo "failed to submit job for schema: $meta_name"
    else
      echo "check UI: http://$ADDRESS:$port for schema: $meta_name, output: $output"
    fi
  fi
done
