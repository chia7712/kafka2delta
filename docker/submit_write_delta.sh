#!/bin/bash

# ===============================[global variables]===============================

declare -r USER=astraea
declare -r IMAGE_NAME="ghcr.io/skiptests/astraea/spark:3.1.2"
declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
declare -r ADDRESS=$([[ "$(which ipconfig)" != "" ]] && ipconfig getifaddr en0 || hostname -i)

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] submit_write_delta.sh"
  echo "Arguments: "
  echo "    --account     gen2 account"
  echo "    --container   gen2 container"
  echo "    --key         gen2 share key"
  echo "    --path        gen2 path"
  echo "    --brokers     kafka bootstrap servers"
  echo "    --metadata    metadata files location"
  echo "    --mode        spark submit mode"
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

function checkOs() {
  if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "This script requires to run container with \"--network host\", but the feature is unsupported by Mac OS"
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
metadata_folder="$DOCKER_FOLDER/../metadata"
main_python="$DOCKER_FOLDER/../write_delta.py"
utils_python="$DOCKER_FOLDER/../utils.py"
use_merge="false"
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

checkImage
requireFolder "$metadata_folder"
requirePythonFile "$main_python"
requirePythonFile "$utils_python"
requireNonEmpty "$brokers" "--brokers is required"

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

for meta_file in "$metadata_folder"/*.xml; do
  filename=$(basename -- "$meta_file")
  container_name="${filename%.*}-delta"
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
      -v "$meta_file":/tmp/schema.xml:ro \
      -v $main_python:/tmp/code/main.py:ro \
      -v $utils_python:/tmp/code/utils.py:ro \
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
      /tmp/code/main.py \
      --output $output \
      --bootstrap_servers $brokers \
      --schema_file /tmp/schema.xml \
      --merge $use_merge >/dev/null 2>&1
    if [ $? -ne 0 ]; then
      echo "failed to submit job for schema: $filename"
    else
      echo "check UI: http://$ADDRESS:$port for schema: $filename, output: $output"
    fi
  fi
done
