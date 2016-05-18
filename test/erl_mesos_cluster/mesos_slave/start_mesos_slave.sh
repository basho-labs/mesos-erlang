#!/bin/bash

MASTER_PATH=/usr/sbin/mesos-master
WORK_DIR_PATH=/var/lib/mesos
PARAMS="--work_dir=${WORK_DIR_PATH}"

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
