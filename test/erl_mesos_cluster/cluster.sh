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

    # Build test executor.
    echo ""
    echo "****************************"
    echo "* Build test executor      *"
    echo "****************************"
    echo ""
    build_test_executor

    # Build mesos slave image.
    echo ""
    echo "****************************"
    echo "* Build mesos slave image *"
    echo "****************************"
    echo ""
    docker build -t erl_mesos_slave "$script_dir"/mesos_slave
}

function build_test_executor {
    script_dir=$(script_dir)
    test_executor_path=$(script_dir)"/../erl_mesos_test_executor"
    rel_path="$test_executor_path/rel/erl_mesos_test_executor"
    target_path="$script_dir/mesos_slave/frameworks_home/erl_mesos_test_executor"
    make rel -C "$test_executor_path"
    tar -zcf "$target_path.tar.gz" -C "$rel_path" .
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
    stop_slave)
        stop_slave
        ;;
    *)
        echo $"Usage: $0 {build|start|stop|restart|stop_master ID|stop_slave}"
        exit 1
esac
