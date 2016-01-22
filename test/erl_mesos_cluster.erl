%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(erl_mesos_cluster).

-export([config/1, config/2]).

-export([start/1,
         stop/1,
         stop_master/2,
         restart/1,
         start_slave/1,
         stop_slave/1]).

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

stop_master(MasterContainer, Config) ->
    ClusterPath = cluster_path(Config),
    os:cmd(ClusterPath ++ " stop_master " ++ MasterContainer).

restart(Config) ->
    ClusterPath = cluster_path(Config),
    os:cmd(ClusterPath ++ " restart").

start_slave(Config) ->
    ClusterPath = cluster_path(Config),
    os:cmd(ClusterPath ++ " start_slave").

stop_slave(Config) ->
    ClusterPath = cluster_path(Config),
    os:cmd(ClusterPath ++ " stop_slave").

%% Internal functions.

test_dir_path(Config) ->
    DataDirPath = proplists:get_value(data_dir, Config),
    filename:dirname(filename:dirname(DataDirPath)).

cluster_path(Config) ->
    filename:join(test_dir_path(Config), ?CLUSTER_PATH).
