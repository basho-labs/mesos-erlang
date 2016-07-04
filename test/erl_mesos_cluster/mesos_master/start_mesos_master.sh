#!/bin/bash

MASTER_PATH=/usr/sbin/mesos-master
PARAMS="--work_dir=${MESOS_WORK_DIR}"
PARAMS="${PARAMS} --log_dir=${MESOS_LOG_DIR}"

mkdir -p ${MESOS_WORK_DIR}
mkdir -p ${MESOS_LOG_DIR}

if [[ "${MESOS_IP}" ]]; then
    PARAMS="${PARAMS} --ip=${MESOS_IP}"
fi

if [[ "${MESOS_HOSTNAME}" ]]; then
    PARAMS="${PARAMS} --hostname=${MESOS_HOSTNAME}"
fi

if [[ "${MESOS_PORT}" ]]; then
    PARAMS="${PARAMS} --port=${MESOS_PORT}"
fi

if [[ "${MESOS_QUORUM}" ]]; then
    PARAMS="${PARAMS} --quorum=${MESOS_QUORUM}"
fi

if [[ "${MESOS_ZK}" ]]; then
    PARAMS="${PARAMS} --zk=${MESOS_ZK}"
fi

${MASTER_PATH} ${PARAMS}
