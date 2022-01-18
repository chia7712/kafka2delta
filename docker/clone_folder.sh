#!/bin/bash

# ===================================[functions]===================================

function showHelp() {
  echo "Usage: [ENV] clone_folder.sh"
  echo "Arguments: "
  echo "    --folder     target folder"
  echo "    --max        max number of clone folders"
  echo "    --interval   sleep time before cloning"
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

folder=""
max="10"
interval="1"
while [[ $# -gt 0 ]]; do
  case $1 in
  --folder)
    folder="$2"
    shift
    shift
    ;;
  --max)
    max="$2"
    shift
    shift
    ;;
  --interval)
    interval="$2"
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

requireNonEmpty "$folder" "folder is required"
requireNonEmpty "$max" "max can not be empty"
requireNonEmpty "$interval" "interval can not be empty"

count=1
while true; do
  for file in "$folder"/*; do
      if [[ -d "$file" ]] && [[ "$file" != *"-clone" ]]; then
        newFile="$file-$(($(($RANDOM % 100000)) + 100000))-clone"
        if [[ ! -d "$newFile" ]]; then
          echo "cloning $file to $newFile for count:$count"
          cp -r $file $newFile
          if [[ "$?" != "0" ]]; then
            echo "failed to copy $file to $newFile"
            exit 2
          fi
          if [[ $count -ge $max ]]; then
            exit 0
          fi
          count=$((count+1))
          sleep $interval
        fi
      fi
  done
done

