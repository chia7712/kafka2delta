#!/bin/bash

# ===============================[global variables]===============================

declare -r DOCKER_FOLDER=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
declare -r ADDRESS=$([[ "$(which ipconfig)" != "" ]] && ipconfig getifaddr en0 || hostname -i)

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] start_kafka_cluster.sh"
  echo "Arguments: "
  echo "    --brokers     number of brokers"
}

# ===================================[main]===================================

brokers=1
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


if [[ -f "$DOCKER_FOLDER/start_zookeeper.sh" ]]; then
  rm -f "$DOCKER_FOLDER/start_zookeeper.sh"
fi

# utils script
curl -H "Cache-Control: no-cache" https://raw.githubusercontent.com/skiptests/astraea/main/docker/docker_build_common.sh -o "$DOCKER_FOLDER/docker_build_common.sh"

curl -H "Cache-Control: no-cache" https://raw.githubusercontent.com/skiptests/astraea/main/docker/start_zookeeper.sh -o "$DOCKER_FOLDER/start_zookeeper.sh"

export ZOOKEEPER_PORT=$(($(($RANDOM % 10000)) + 10000))
bash $DOCKER_FOLDER/start_zookeeper.sh


curl -H "Cache-Control: no-cache" https://raw.githubusercontent.com/skiptests/astraea/main/docker/start_broker.sh -o "$DOCKER_FOLDER/start_broker.sh"

# shellcheck disable=SC2051
# shellcheck disable=SC2034
for ((i=1;i<="$brokers";i++)); do
   bash $DOCKER_FOLDER/start_broker.sh "zookeeper.connect=$ADDRESS:$ZOOKEEPER_PORT"
done
