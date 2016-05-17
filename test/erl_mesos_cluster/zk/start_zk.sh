#!/bin/bash

sed -e "s/tickTime=[0-9]*/tickTime=${ZK_TICK_TIME}/" /etc/zookeeper/conf/zoo.cfg > zoo.cfg.tmp && mv zoo.cfg.tmp /etc/zookeeper/conf/zoo.cfg
sed -e "s/initLimit=[0-9]*/initLimit=${ZK_INIT_LIMIT}/" /etc/zookeeper/conf/zoo.cfg > zoo.cfg.tmp && mv zoo.cfg.tmp /etc/zookeeper/conf/zoo.cfg
sed -e "s/syncLimit=[0-9]*/syncLimit=${ZK_SYNC_LIMIT}/" /etc/zookeeper/conf/zoo.cfg > zoo.cfg.tmp && mv zoo.cfg.tmp /etc/zookeeper/conf/zoo.cfg
sed -e "s/clientPort=[0-9]*/clientPort=${ZK_CLIENT_PORT}/" /etc/zookeeper/conf/zoo.cfg > zoo.cfg.tmp && mv zoo.cfg.tmp /etc/zookeeper/conf/zoo.cfg

/usr/share/zookeeper/bin/zkServer.sh start-foreground
