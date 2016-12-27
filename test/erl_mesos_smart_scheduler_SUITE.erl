%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License. You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(erl_mesos_smart_scheduler_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2]).

-export([bad_options/1]).

all() ->
    [bad_options].

groups() ->
    [{mesos_cluster, []}].

init_per_suite(Config) ->
    {ok, _Apps} = application:ensure_all_started(erl_mesos),
    Scheduler = erl_mesos_test_smart_scheduler,
    SchedulerOptions = [{user, "root"},
                        {name, "erl_mesos_smart_test_scheduler"}],
    {ok, Masters} = erl_mesos_cluster:config(masters, Config),
    MasterHosts = [MasterHost || {_Container, MasterHost} <- Masters],
    Options = [{master_hosts, MasterHosts}],
    [{scheduler, Scheduler},
     {scheduler_options, SchedulerOptions},
     {options, Options} |
     Config].

end_per_suite(_Config) ->
    ok.

init_per_group(mesos_cluster, Config) ->
    Config.

end_per_group(mesos_cluster, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    process_flag(trap_exit, true),
    case lists:member(TestCase, proplists:get_value(mesos_cluster, groups())) of
        true ->
            stop_mesos_cluster(Config),
            start_mesos_cluster(Config),
            Config;
        false ->
            Config
    end.

end_per_testcase(TestCase, Config) ->
    case lists:member(TestCase, proplists:get_value(mesos_cluster, groups())) of
        true ->
            stop_mesos_cluster(Config),
            Config;
        false ->
            Config
    end.

%% Test functions.

bad_options(Config) ->
    Name = erl_mesos_scheduler_bad_options,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    %% Bad options.
    Options = undefined,
    {error, {bad_options, Options}} =
        start_smart_scheduler(Name, Scheduler, SchedulerOptions, Options),
    %% Bad max number of execute.
    MaxNumExecute = undefined,
    Options1 = [{max_num_execute, MaxNumExecute}],
    {error, {bad_max_num_execute, MaxNumExecute}} =
        start_smart_scheduler(Name, Scheduler, SchedulerOptions, Options1),
    %% Bad max queue length.
    BadMaxQueueLength = undefined,
    Options2 = [{max_queue_length, BadMaxQueueLength}],
    {error, {bad_max_queue_length, BadMaxQueueLength}} =
        start_smart_scheduler(Name, Scheduler, SchedulerOptions, Options2).

%% Internal functions.

start_mesos_cluster(Config) ->
    erl_mesos_cluster:start(Config),
    {ok, StartTimeout} = erl_mesos_cluster:config(start_timeout, Config),
    timer:sleep(StartTimeout).

stop_mesos_cluster(Config) ->
    erl_mesos_cluster:stop(Config).

start_smart_scheduler(Name, Scheduler, SchedulerOptions, Options) ->
    erl_mesos_smart_scheduler:start_link(Name, Scheduler, SchedulerOptions,
                                         Options).
