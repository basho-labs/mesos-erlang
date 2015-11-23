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
    *)
        echo $"Usage: $0 {start|stop|restart}"
        exit 1
esac
