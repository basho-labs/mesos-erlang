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
function build {
    script_dir=$(script_dir)

    # Build zookeeper image.
    echo ""
    echo "*************************"
    echo "* Build zookeeper image *"
    echo "*************************"
    echo ""
    docker build -t erl_mesos_zk "$script_dir"/zk

    # Build mesos master image.
    echo ""
    echo "****************************"
    echo "* Build mesos master image *"
    echo "****************************"
    echo ""
    docker build -t erl_mesos_master "$script_dir"/mesos_master

    # Build mesos slave image.
    echo ""
    echo "****************************"
    echo "* Build mesos slave image *"
    echo "****************************"
    echo ""
    docker build -t erl_mesos_slave "$script_dir"/mesos_slave
}

function start {
    docker_compose_path=$(script_dir)"/cluster.yml"
    docker-compose -f "$docker_compose_path" up -d
}

function stop {
    docker_compose_path=$(script_dir)"/cluster.yml"
    docker-compose -f "$docker_compose_path" kill
    docker-compose -f "$docker_compose_path" rm -f --all
}

function restart {
    stop
    start
}

function stop_master {
    docker kill "$1"
}

function start_slave {
    docker run --privileged --name=erl_mesos_slave\
               --link=erl_mesos_zk:erl_mesos_zk -d\
               -e MESOS_MASTER=zk://erl_mesos_zk:2181/mesos\
               erl_mesos_slave
}

function stop_slave {
    docker kill erl_mesos_slave
    docker rm erl_mesos_slave
}

case "$1" in
    build)
        build
        ;;
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
    start_slave)
        start_slave
        ;;
    stop_slave)
        stop_slave
        ;;
    *)
        echo $"Usage: $0 {build|start|stop|restart|stop_master ID|start_slave|stop_slave}"
        exit 1
esac
