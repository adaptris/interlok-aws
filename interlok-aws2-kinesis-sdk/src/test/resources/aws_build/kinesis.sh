#! /usr/bin/env bash
set -eu

if ! [ -x "$(command -v jq)" ]; then
  if [ -x "$(command -v apk)" ]; then
    apk add jq
  else
    "JQ not avaiable"
    exit 1
  fi
fi


export PYTHONWARNINGS=ignore

function newStream()
{
  local streamName=$1
  awslocal kinesis create-stream --stream-name "$streamName" --shard-count 1
}

function getShardId()
{
  local streamName=$1
  shardId=$(awslocal kinesis list-shards --stream-name "$streamName" | jq -r ".Shards[0].ShardId")
  echo $shardId
}

function getShardIterator()
{
  local streamName=$1
  iteratorId=$(awslocal kinesis get-shard-iterator --stream-name $streamName --shard-id $(getShardId $streamName) --shard-iterator-type LATEST | jq -r ".ShardIterator")
  echo $iteratorId
}

function getRecords()
{
  local streamName=$1
  local iteratorId=$2
  iteratorId=${iteratorId:=$(getShardIterator $streamName)}
  awslocal kinesis get-records --shard-iterator "$iteratorId"
}

function loopUntilKilled() {
  local streamName=$1
  iteratorId=$(getShardIterator "$streamName")
  while true; do
    getRecords myStream "$iteratorId"
    sleep 5
  done
}

STREAM_NAME=${1:-my-stream}
ACTION=${2:-create}

if [ "$ACTION" == "create" ]
then
  newStream "$STREAM_NAME"
  ## Sleep a couple of seconds, since the stream creation takes time
  sleep 2
  awslocal kinesis list-streams
fi
loopUntilKilled $STREAM_NAME
