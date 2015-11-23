#!/bin/bash

# Build zookeeper.
echo "*************************"
echo "* Build zookeeper image *"
echo "*************************"
docker build -t zk test/mesos_cluster/zookeeper

# Build mesos master.
echo "****************************"
echo "* Build mesos master image *"
echo "****************************"
docker build -t mesos_master test/mesos_cluster/mesos_master
