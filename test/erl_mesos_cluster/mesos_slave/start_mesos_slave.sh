#!/bin/bash

SLAVE_PATH=/usr/sbin/mesos-slave
PARAMS="--frameworks_home=${MESOS_FRAMEWORKS_HOME}"
PARAMS="${PARAMS} --log_dir=${MESOS_LOG_DIR}"

mkdir -p ${MESOS_FRAMEWORKS_HOME}
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

if [[ "${MESOS_MASTER}" ]]; then
    PARAMS="${PARAMS} --master=${MESOS_MASTER}"
fi

${SLAVE_PATH} ${PARAMS}
