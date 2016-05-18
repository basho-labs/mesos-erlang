#!/bin/bash

CFG_PATH=/etc/zookeeper/conf/zoo.cfg
SERVER_PATH=/usr/share/zookeeper/bin/zkServer.sh

sed -e "s/tickTime=[0-9]*/tickTime=${ZK_TICK_TIME}/" ${CFG_PATH} > ${CFG_PATH}.tmp && mv ${CFG_PATH}.tmp ${CFG_PATH}
sed -e "s/initLimit=[0-9]*/initLimit=${ZK_INIT_LIMIT}/" ${CFG_PATH} > ${CFG_PATH}.tmp && mv ${CFG_PATH}.tmp ${CFG_PATH}
sed -e "s/syncLimit=[0-9]*/syncLimit=${ZK_SYNC_LIMIT}/" ${CFG_PATH} > ${CFG_PATH}.tmp && mv ${CFG_PATH}.tmp ${CFG_PATH}
sed -e "s/clientPort=[0-9]*/clientPort=${ZK_CLIENT_PORT}/" ${CFG_PATH} > ${CFG_PATH}.tmp && mv ${CFG_PATH}.tmp ${CFG_PATH}

${SERVER_PATH} start-foreground
