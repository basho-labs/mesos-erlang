#!/bin/bash

SLAVE_PATH=/usr/sbin/mesos-slave
PARAMS=""
PARAMS="${PARAMS} --resources=${MESOS_RESOURCES}"
PARAMS="${PARAMS} --isolation=${MESOS_ISOLATION}"
PARAMS="${PARAMS} --attributes=${MESOS_ATTRIBUTES}"
PARAMS="${PARAMS} --frameworks_home=${MESOS_FRAMEWORKS_HOME}"

mkdir -p ${MESOS_FRAMEWORKS_HOME}

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
