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
    *)
        echo $"Usage: $0 {start|stop|restart|stop_master ID}"
        exit 1
esac
