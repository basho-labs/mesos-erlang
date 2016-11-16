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

-module(erl_mesos_executor_SUITE).

-include_lib("common_test/include/ct.hrl").

-include_lib("erl_mesos_executor_info.hrl").

-include_lib("erl_mesos_executor_proto.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2]).

-export([registered/1,
         reregistered/1,
         launch_task/1,
         kill_task/1,
         acknowledged/1,
         framework_message/1,
         shutdown/1,
         handle_info/1,
         terminate/1]).

all() ->
    [{group, mesos_cluster, [sequence]}].

groups() ->
    [{mesos_cluster, [registered,
                      reregistered,
                      launch_task,
                      kill_task,
                      acknowledged,
                      framework_message,
                      shutdown,
                      handle_info,
                      terminate]}].

init_per_suite(Config) ->
    {ok, _Apps} = application:ensure_all_started(erl_mesos),
    Scheduler = erl_mesos_test_scheduler,
    SchedulerOptions = [{user, "root"}, {name, "erl_mesos_test_scheduler"}],
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

registered(Config) ->
    Name = erl_mesos_executor_registered,
    start_and_accept_scheduler(Name, Config),
    {registered, {ExecutorInfo, EventSubscribed}} =
        recv_framework_message_reply(registered),
    %% Test executor info.
    #executor_info{subscribed = true} = ExecutorInfo,
    %% Test event subscribed.
    true = is_record(EventSubscribed, 'Event.Subscribed'),
    ok = stop_scheduler(Name).

reregistered(Config) ->
    Name = erl_mesos_executor_reregistered,
    {SchedulerPid, AgentId, TaskId} =
        start_and_accept_scheduler(Name, Config),
    {status_update, {SchedulerPid, _SchedulerInfo, _EventUpdate}} =
        recv_reply(status_update),
    ExecutorId = #'ExecutorID'{value = TaskId#'TaskID'.value},
    SchedulerPid ! {disconnect_executor, AgentId, ExecutorId},
    {reregistered,
     {ExecutorInfo, DisconnectedExecutorInfo, ReregisterExecutorInfo}} =
        recv_framework_message_reply(reregistered),
    %% Test executor info.
    #executor_info{subscribed = true} = ExecutorInfo,
    %% Test disconnected executor info.
    #executor_info{subscribed = false} = DisconnectedExecutorInfo,
    %% Test reregister executor info.
    #executor_info{subscribed = false} = ReregisterExecutorInfo,
    ok = stop_scheduler(Name).

launch_task(Config) ->
    Name = erl_mesos_executor_launch_task,
    {SchedulerPid, _AgentId, _TaskId} =
        start_and_accept_scheduler(Name, Config),
    {status_update, {SchedulerPid, _SchedulerInfo, _EventUpdate}} =
        recv_reply(status_update),
    {launch_task, {Update, ExecutorInfo}} =
        recv_framework_message_reply(launch_task),
    %% Test update.
    ok = Update,
    %% Test executor info.
    #executor_info{subscribed = true} = ExecutorInfo,
    ok = stop_scheduler(Name).

kill_task(Config) ->
    Name = erl_mesos_executor_kill_task,
    {SchedulerPid, _AgentId, TaskId} =
        start_and_accept_scheduler(Name, Config),
    {status_update, {SchedulerPid, _SchedulerInfo, _EventUpdate}} =
        recv_reply(status_update),
    SchedulerPid ! {kill, TaskId},
    {kill_task, {ExecutorInfo, EventKill}} =
        recv_framework_message_reply(kill_task),
    %% Test executor info.
    #executor_info{subscribed = true} = ExecutorInfo,
    %% Test event kill.
    #'Event.Kill'{task_id = TaskId} = EventKill,
    ok = stop_scheduler(Name).

acknowledged(Config) ->
    Name = erl_mesos_executor_acknowledged,
    {SchedulerPid, _AgentId, TaskId} =
        start_and_accept_scheduler(Name, Config),
    {status_update, {SchedulerPid, _SchedulerInfo, _EventUpdate}} =
        recv_reply(status_update),
    {acknowledged, {ExecutorInfo, EventAcknowledged}} =
        recv_framework_message_reply(acknowledged),
    %% Test executor info.
    #executor_info{subscribed = true} = ExecutorInfo,
    %% Test event acknowledged.
    #'Event.Acknowledged'{task_id = TaskId} = EventAcknowledged,
    ok = stop_scheduler(Name).

framework_message(Config) ->
    Name = erl_mesos_executor_framework_message,
    {SchedulerPid, AgentId, TaskId} =
        start_and_accept_scheduler(Name, Config),
    {status_update, {SchedulerPid, _SchedulerInfo, _EventUpdate}} =
        recv_reply(status_update),
    ExecutorId = #'ExecutorID'{value = TaskId#'TaskID'.value},
    Data = <<"testdata">>,
    SchedulerPid ! {message, AgentId, ExecutorId, Data},
    {framework_message, {ExecutorInfo, EventMessage}} =
        recv_framework_message_reply(framework_message),
    %% Test executor info.
    #executor_info{subscribed = true} = ExecutorInfo,
    %% Test event message
    #'Event.Message'{data = Data} = EventMessage,
    ok = stop_scheduler(Name).

shutdown(Config) ->
    Name = erl_mesos_executor_shutdown,
    {SchedulerPid, AgentId, TaskId} =
        start_and_accept_scheduler(Name, Config),
    {status_update, {SchedulerPid, _SchedulerInfo, _EventUpdate}} =
        recv_reply(status_update),
    ExecutorId = #'ExecutorID'{value = TaskId#'TaskID'.value},
    SchedulerPid ! {shutdown, ExecutorId, AgentId},
    {shutdown, ExecutorInfo} = recv_framework_message_reply(shutdown),
    %% Test executor info.
    #executor_info{subscribed = true} = ExecutorInfo,
    ok = stop_scheduler(Name).

handle_info(Config) ->
    Name = erl_mesos_executor_handle_info,
    {SchedulerPid, AgentId, TaskId} =
        start_and_accept_scheduler(Name, Config),
    {status_update, {SchedulerPid, _SchedulerInfo, _EventUpdate}} =
        recv_reply(status_update),
    ExecutorId = #'ExecutorID'{value = TaskId#'TaskID'.value},
    SchedulerPid ! {info_executor, AgentId, ExecutorId},
    {handle_info, {ExecutorInfo, Info}} =
        recv_framework_message_reply(handle_info),
    %% Test executor info.
    #executor_info{subscribed = true} = ExecutorInfo,
    %% Test info.
    info = Info,
    ok = stop_scheduler(Name).

terminate(Config) ->
    Name = erl_mesos_executor_terminate,
    {SchedulerPid, AgentId, TaskId} =
        start_and_accept_scheduler(Name, Config),
    {status_update, {SchedulerPid, _SchedulerInfo, _EventUpdate}} =
        recv_reply(status_update),
    ExecutorId = #'ExecutorID'{value = TaskId#'TaskID'.value},
    SchedulerPid ! {stop_executor, AgentId, ExecutorId},
    {terminate, {ExecutorInfo, Reason}} = recv_framework_message_reply(terminate),
    %% Test executor info.
    #executor_info{subscribed = true} = ExecutorInfo,
    %% Test reason.
    shutdown = Reason,
    ok = stop_scheduler(Name).

%% Internal functions.

start_mesos_cluster(Config) ->
    erl_mesos_cluster:start(Config),
    {ok, StartTimeout} = erl_mesos_cluster:config(start_timeout, Config),
    timer:sleep(StartTimeout).

stop_mesos_cluster(Config) ->
    erl_mesos_cluster:stop(Config).

start_scheduler(Name, Scheduler, SchedulerOptions, Options) ->
    erl_mesos_scheduler:start_link(Name, Scheduler, SchedulerOptions, Options).

stop_scheduler(Name) ->
    erl_mesos_scheduler:stop(Name).

start_and_accept_scheduler(Name, Config) ->
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    [Offer | _] = offers(EventOffers),
    #'Offer'{id = OfferId, agent_id = AgentId} = Offer,
    TaskId = timestamp_task_id(),
    SchedulerPid ! {accept, OfferId, AgentId, TaskId},
    {accept, ok} = recv_reply(accept),
    {SchedulerPid, AgentId, TaskId}.

set_test_pid(SchedulerOptions) ->
    [{test_pid, self()} | SchedulerOptions].

offers(EventOffers) ->
    erl_mesos_test_utils:offers(EventOffers).

recv_reply(Reply) ->
    erl_mesos_test_utils:recv_reply(Reply).

recv_framework_message_reply(Reply) ->
    erl_mesos_test_utils:recv_framework_message_reply(Reply).

timestamp_task_id() ->
    erl_mesos_test_utils:timestamp_task_id().

