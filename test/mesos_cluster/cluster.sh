#!/bin/bash

function script_dir {
    echo "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
}

function start {
    docker_compose_path=$(script_dir)"/docker-compose.yml"
    docker-compose -f "$docker_compose_path" up -d
}

function stop {
    docker_compose_path=$(script_dir)"/docker-compose.yml"
    docker-compose -f "$docker_compose_path" stop
    docker-compose -f "$docker_compose_path" rm -f
}

function restart {
    stop
    start
}

function stop_master {
    docker stop "mesos_master_${1}"
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
    stop_master_1)
        stop_master "1"
        ;;
    stop_master_2)
        stop_master "2"
        ;;
    stop_master_3)
        stop_master "3"
        ;;
    *)
        echo $"Usage: $0 {start|stop|restart}"
        exit 1
esac
