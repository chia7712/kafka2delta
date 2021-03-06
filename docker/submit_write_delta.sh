#!/bin/bash

# ===============================[read utils]===============================
source "$(dirname "$0")"/utils.sh
# ===============================[global variables]===============================
declare -r IMAGE_NAME="ghcr.io/chia7712/kafka2delta/k2d:$BASE_VERSION"
declare -r DOCKERFILE=$DOCKER_FOLDER/k2d.dockerfile
# ===============================[path in container]===============================
declare -r CODE_FOLDER_IN_CONTAINER="/tmp/code"
declare -r MAIN_PATH_IN_CONTAINER="$CODE_FOLDER_IN_CONTAINER/write_delta.py"
declare -r METADATA_FOLDER_IN_CONTAINER="/tmp/metadata"
# ===============================[driver/executor resource]===============================
declare -r RESOURCES_CONFIGS="--conf spark.driver.cores=1 \
                      --conf spark.driver.memory=2g \
                      --conf spark.executor.cores=2 \
                      --conf spark.executor.memory=4g \
                      --conf spark.executor.instances=2"
# add more memory to driver to run both driver/executor in local mode
declare -r RESOURCES_CONFIGS_FOR_LOCAL="--conf spark.driver.memory=6g"
# ===================================[functions]===================================
function showHelp() {
  echo "Usage: [ENV] submit_write_delta.sh"
  echo "Arguments: "
  echo "    --account         gen2 account"
  echo "    --container       gen2 container"
  echo "    --key             gen2 share key"
  echo "    --path            gen2 path"
  echo "    --brokers         kafka bootstrap servers"
  echo "    --master          spark submit mode"
  echo "    --k8s_account     account of k8s (required when you want to submit job on k8s)"
  echo "    --k8s_namespace   namespace of k8s (required when you want to submit job on k8s)"
}

function buildImageIfNeed() {
  local master=$1
  generateDockerfileForCode "$IMAGE_NAME" "$DOCKERFILE" "$master"

  docker build --no-cache -t "$IMAGE_NAME" -f "$DOCKERFILE" "$DOCKER_FOLDER"
  if [[ "$?" != "0" ]]; then
    exit 2
  fi

  if [[ "$master" == *"k8s"* ]]; then
    # make all k8s nodes able to download image
    docker push $IMAGE_NAME
    if [[ "$?" != "0" ]]; then
      exit 2
    fi
  fi
}
# ===================================[main]===================================
gen2_account=""
gen2_container=""
gen2_key=""
path=""
master="local[*]"
brokers=""
k8s_namespace="default"
k8s_account="default"
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
  --master)
    master="$2"
    shift
    shift
    ;;
  --k8s_namespace)
    k8s_namespace="$2"
    shift
    shift
    ;;
  --k8s_account)
    k8s_account="$2"
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

# the non-local mode requires "--network=host", but it is unsupported by Mac OS
if [[ "$master" != *"local"* ]]; then
  checkOs
fi
requireFolder "$CODE_FOLDER"
requireFolder "$METADATA_FOLDER"
checkGen2Args "$gen2_account" "$gen2_container" "$gen2_key"
requireNonEmpty "$path" "--path is required"
requireNonEmpty "$brokers" "--brokers is required"
downloadBaseImage
buildImageIfNeed "$master"

if [[ "$gen2_account" != "" ]] && [[ "$gen2_container" != "" ]] && [[ "$gen2_key" != "" ]]; then
  gen2_configs=$(generateGen2Configs "$gen2_account" "$gen2_container" "$gen2_key")
  output=$(generateGen2Path "$gen2_account" "$gen2_container" "$path")
  echo "write data to azure: $output"
else
  if [[ "$master" != *"local"* ]]; then
    echo "Please use gen2 path in non-local mode"
    exit 2
  fi
  mkdir -p "$path"
  output="/tmp/output"
  volume_configs="-v $path:/tmp/output"
  echo "write data to local: $path"
fi

# reference to local file so we don't need to share those files between all pods
main_path=$([[ "$master" == "k8s"* ]] && echo "local://$MAIN_PATH_IN_CONTAINER" || echo "$MAIN_PATH_IN_CONTAINER")
k8s_configs=$(generateK8sConfigs "$k8s_namespace" "$k8s_account" "$IMAGE_NAME")
resource=$([[ "$master" == *"local"* ]] && echo "$RESOURCES_CONFIGS_FOR_LOCAL" || echo "$RESOURCES_CONFIGS")
for meta_file in "$METADATA_FOLDER"/*.xml; do
  meta_name=$(basename -- "$meta_file")
  container_name="${meta_name%.*}-delta"
  if [[ "$(docker ps --format={{.Names}} | grep -w $container_name)" != "" ]]; then
    echo "container: $container_name is already running"
    continue
  fi
  ui_port=$(($(($RANDOM%10000))+10000))
  if [[ "$master" == *"local"* ]]; then
    network_config="-p ${ui_port}:${ui_port}"
  else
    network_config="--network host"
  fi
  docker run -d \
    --name $container_name \
    $network_config \
    $volume_configs \
    $IMAGE_NAME \
    ./bin/spark-submit \
    --name $container_name \
    $gen2_configs \
    $DELTA_CONFIGS \
    $resource \
    $k8s_configs \
    --conf "spark.ui.port=$ui_port" \
    --deploy-mode client \
    --master $master \
    "$main_path" \
    --output $output \
    --bootstrap_servers $brokers \
    --schema_file $METADATA_FOLDER_IN_CONTAINER/$meta_name >/dev/null 2>&1
  if [ $? -ne 0 ]; then
    echo "failed to submit job for schema: $meta_name"
  else
    echo "check UI: http://$ADDRESS:$ui_port for schema: $meta_name, data location: $path"
  fi
done
