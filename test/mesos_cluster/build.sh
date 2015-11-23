#!/bin/bash

function script_dir {
    echo "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
}

script_dir=$(script_dir)

# Build zookeeper.
echo "*************************"
echo "* Build zookeeper image *"
echo "*************************"
docker build -t zk "$script_dir"/zookeeper

# Build mesos master.
echo "****************************"
echo "* Build mesos master image *"
echo "****************************"
docker build -t mesos_master "$script_dir"/mesos_master
