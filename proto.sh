#!/bin/bash

VSN=$1
PARENT=$(pwd)
URL_PREFIX=https://raw.githubusercontent.com/apache/mesos

# Download proto files
mkdir -p ${PARENT}/proto/mesos/v1/scheduler
wget -q ${URL_PREFIX}/${VSN}/include/mesos/v1/mesos.proto -P ${PARENT}/proto/mesos/v1
wget -q ${URL_PREFIX}/${VSN}/include/mesos/v1/scheduler/scheduler.proto -P ${PARENT}/proto/mesos/v1/scheduler

SCHEDULER=${PARENT}/proto/mesos/v1/scheduler/scheduler.proto

# Fix scheduler.proto
sed -e 's/message Request/message Req/' ${SCHEDULER} > ${SCHEDULER}.tmp && mv ${SCHEDULER}.tmp ${SCHEDULER}
sed -e 's/repeated mesos.v1.Request/repeated Request/' ${SCHEDULER} > ${SCHEDULER}.tmp && mv ${SCHEDULER}.tmp ${SCHEDULER}
sed -e 's/optional Request/optional Req/' ${SCHEDULER} > ${SCHEDULER}.tmp && mv ${SCHEDULER}.tmp ${SCHEDULER}

# Compile scheduler.proto
erl +B -noinput -pa ${PARENT}/deps/gpb/ebin\
    -I${PARENT}/proto -o-erl src -o-hrl include -modsuffix _protobuf -il\
    -s gpb_compile c ${SCHEDULER}
