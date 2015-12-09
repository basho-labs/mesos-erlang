#!/bin/bash

#
# Private functions.
#
function script_dir {
    echo "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
}

#
# Public functions.
#
function start {
    docker_compose_path=$(script_dir)"/cluster.yml"
    docker-compose -f "$docker_compose_path" up -d
}

function stop {
    docker_compose_path=$(script_dir)"/cluster.yml"
    # We should use kill here for immediately cluster stop.
    docker-compose -f "$docker_compose_path" kill
    docker-compose -f "$docker_compose_path" rm -f
}

function restart {
    stop
    start
}

function stop_master {
    docker stop "$1"
}

function start_empty_slave {
    docker run --privileged\
               --name=mesos_empty_slave \
               --link=zk:zk\
               -d\
               -e\
               conf=--master=zk://zk:2181/mesos\
               mesos_slave
}

function stop_empty_slave {
    docker kill mesos_empty_slave
    docker rm mesos_empty_slave
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    stop_master)
        stop_master "$2"
        ;;
    start_empty_slave)
        start_empty_slave
        ;;
    stop_empty_slave)
        stop_empty_slave
        ;;
    *)
        echo $"Usage: $0 {start|stop|restart|stop_master ID|start_empty_slave|stop_empty_slave}"
        exit 1
esac
