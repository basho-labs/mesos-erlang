-module(erl_mesos_cluster).

-export([config/1, config/2]).

-export([start/1, stop/1, restart/1]).

-define(CLUSTER_PATH, "mesos_cluster/cluster.sh").

%% External functions.

config(Config) ->
    file:consult(filename:join(test_dir_path(Config), "cluster.config")).

config(masters, Config) ->
    case config(Config) of
        {ok, Terms} ->
            {ok, proplists:get_value(masters, Terms)};
        {error, Reason} ->
            {error, Reason}
    end.

start(Config) ->
    ClusterPath = cluster_path(Config),
    os:cmd(ClusterPath ++ " start").

stop(Config) ->
    ClusterPath = cluster_path(Config),
    os:cmd(ClusterPath ++ " stop").

restart(Config) ->
    ClusterPath = cluster_path(Config),
    os:cmd(ClusterPath ++ " restart").

%% Internal functions.

test_dir_path(Config) ->
    DataDirPath = proplists:get_value(data_dir, Config),
    filename:dirname(filename:dirname(DataDirPath)).

cluster_path(Config) ->
    filename:join(test_dir_path(Config), ?CLUSTER_PATH).
