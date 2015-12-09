-module(erl_mesos_cluster).

-export([config/1, config/2]).

-export([start/1,
         stop/1,
         stop_master/2,
         restart/1,
         start_empty_slave/1,
         stop_empty_slave/1]).

-define(CLUSTER_PATH, "mesos_cluster/cluster.sh").

%% External functions.

config(Config) ->
    file:consult(filename:join(test_dir_path(Config), "cluster.config")).

config(Key, Config) ->
    case config(Config) of
        {ok, Terms} ->
            case proplists:get_value(Key, Terms) of
                undefined ->
                    {error, not_found};
                Value ->
                    {ok, Value}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

start(Config) ->
    ClusterPath = cluster_path(Config),
    os:cmd(ClusterPath ++ " start").

stop(Config) ->
    ClusterPath = cluster_path(Config),
    os:cmd(ClusterPath ++ " stop").

stop_master(Config, MasterContainer) ->
    ClusterPath = cluster_path(Config),
    os:cmd(ClusterPath ++ " stop_master " ++ MasterContainer).

restart(Config) ->
    ClusterPath = cluster_path(Config),
    os:cmd(ClusterPath ++ " restart").

start_empty_slave(Config) ->
    ClusterPath = cluster_path(Config),
    os:cmd(ClusterPath ++ " start_empty_slave").

stop_empty_slave(Config) ->
    ClusterPath = cluster_path(Config),
    os:cmd(ClusterPath ++ " stop_empty_slave").

%% Internal functions.

test_dir_path(Config) ->
    DataDirPath = proplists:get_value(data_dir, Config),
    filename:dirname(filename:dirname(DataDirPath)).

cluster_path(Config) ->
    filename:join(test_dir_path(Config), ?CLUSTER_PATH).
