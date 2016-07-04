#!/bin/bash

if [ -z "$HOME" ]; then
    ID=$(id -un)
    export HOME=$(eval echo "~${ID}")
fi

./bin/erl_mesos_test_executor foreground
