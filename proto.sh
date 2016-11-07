#!/bin/bash

VSN=$1
PARENT=$(pwd)
URL_PREFIX=https://raw.githubusercontent.com/apache/mesos

# Download proto files
mkdir -p ${PARENT}/proto/mesos/v1/scheduler
wget -q -N ${URL_PREFIX}/${VSN}/include/mesos/v1/mesos.proto -P ${PARENT}/proto/mesos/v1
wget -q -N ${URL_PREFIX}/${VSN}/include/mesos/v1/quota/quota.proto -P ${PARENT}/proto/mesos/v1/quota
wget -q -N ${URL_PREFIX}/${VSN}/include/mesos/v1/allocator/allocator.proto -P ${PARENT}/proto/mesos/v1/allocator
wget -q -N ${URL_PREFIX}/${VSN}/include/mesos/v1/maintenance/maintenance.proto -P ${PARENT}/proto/mesos/v1/maintenance
wget -q -N ${URL_PREFIX}/${VSN}/include/mesos/v1/master/master.proto -P ${PARENT}/proto/mesos/v1/master
wget -q -N ${URL_PREFIX}/${VSN}/include/mesos/v1/agent/agent.proto -P ${PARENT}/proto/mesos/v1/agent
wget -q -N ${URL_PREFIX}/${VSN}/include/mesos/v1/scheduler/scheduler.proto -P ${PARENT}/proto/mesos/v1/scheduler
wget -q -N ${URL_PREFIX}/${VSN}/include/mesos/v1/executor/executor.proto -P ${PARENT}/proto/mesos/v1/executor

MASTER=${PARENT}/proto/mesos/v1/master/master.proto
AGENT=${PARENT}/proto/mesos/v1/agent/agent.proto
MAINT=${PARENT}/proto/mesos/v1/maintenance/maintenance.proto
SCHEDULER=${PARENT}/proto/mesos/v1/scheduler/scheduler.proto
EXECUTOR=${PARENT}/proto/mesos/v1/executor/executor.proto

# Fix mater.proto
sed -e 's/required maintenance.Schedule/repeated Schedule/' ${MASTER} > ${MASTER}.tmp && mv ${MASTER}.tmp ${MASTER}
sed -e 's/required quota.QuotaRequest/required QuotaRequest/' ${MASTER} > ${MASTER}.tmp && mv ${MASTER}.tmp ${MASTER}
sed -e 's/required maintenance.ClusterStatus/required ClusterStatus/' ${MASTER} > ${MASTER}.tmp && mv ${MASTER}.tmp ${MASTER}
sed -e 's/required quota.QuotaStatus/required QuotaStatus/' ${MASTER} > ${MASTER}.tmp && mv ${MASTER}.tmp ${MASTER}

# Fix maintenance.proto
sed -e 's/repeated allocator.InverseOfferStatus/repeated InverseOfferStatus/' ${MAINT} > ${MAINT}.tmp && mv ${MAINT}.tmp ${MAINT}

# Fix scheduler.proto
sed -e 's/message Request/message Req/' ${SCHEDULER} > ${SCHEDULER}.tmp && mv ${SCHEDULER}.tmp ${SCHEDULER}
sed -e 's/repeated mesos.v1.Request/repeated Request/' ${SCHEDULER} > ${SCHEDULER}.tmp && mv ${SCHEDULER}.tmp ${SCHEDULER}
sed -e 's/optional Request/optional Req/' ${SCHEDULER} > ${SCHEDULER}.tmp && mv ${SCHEDULER}.tmp ${SCHEDULER}

# Compile master.proto
erl +B -noinput -pa ${PARENT}/deps/gpb/ebin\
    -I${PARENT}/proto -o-erl src -o-hrl include -modsuffix _protobuf -il\
    -s gpb_compile c ${MASTER}

# Compile agent.proto
erl +B -noinput -pa ${PARENT}/deps/gpb/ebin\
    -I${PARENT}/proto -o-erl src -o-hrl include -modsuffix _protobuf -il\
    -s gpb_compile c ${AGENT}

# Compile scheduler.proto
erl +B -noinput -pa ${PARENT}/deps/gpb/ebin\
    -I${PARENT}/proto -o-erl src -o-hrl include -modsuffix _protobuf -il\
    -s gpb_compile c ${SCHEDULER}

# Compile executor.proto
erl +B -noinput -pa ${PARENT}/deps/gpb/ebin\
    -I${PARENT}/proto -o-erl src -o-hrl include -modsuffix _protobuf -il\
    -s gpb_compile c ${EXECUTOR}
