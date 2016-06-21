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

-module(erl_mesos_test_executor).

-behaviour(erl_mesos_executor).

-include_lib("executor_info.hrl").

-include_lib("executor_protobuf.hrl").

-export([init/1,
         registered/3,
         disconnected/2,
         reregister/2,
         reregistered/2,
         launch_task/3,
         kill_task/3,
         acknowledged/3,
         framework_message/3,
         error/3,
         shutdown/2,
         handle_info/3,
         terminate/3]).

%% erl_mesos_executor callback functions.

init(_Options) ->
    {ok, #'Call.Subscribe'{}, undefined}.

registered(ExecutorInfo, EventSubscribed, State) ->
    reply(ExecutorInfo, registered, {ExecutorInfo, EventSubscribed}),
    {ok, State}.

disconnected(ExecutorInfo, State) ->
    reply(ExecutorInfo, disconnected, ExecutorInfo),
    {ok, State}.

reregister(ExecutorInfo, State) ->
    reply(ExecutorInfo, reregister, ExecutorInfo),
    {ok, #'Call.Subscribe'{}, State}.

reregistered(ExecutorInfo, State) ->
    reply(ExecutorInfo, reregistered, ExecutorInfo),
    {ok, State}.

launch_task(ExecutorInfo, #'Event.Launch'{task = TaskInfo}, State) ->
    #'TaskInfo'{task_id = TaskId} = TaskInfo,
    TaskStatus = #'TaskStatus'{task_id = TaskId,
                               state = 'TASK_RUNNING',
                               uuid = uuid(),
                               source = 'SOURCE_EXECUTOR'},
    Update = erl_mesos_executor:update(ExecutorInfo, TaskStatus),
    reply(ExecutorInfo, launch_task, {Update, ExecutorInfo}),
    {ok, State}.

kill_task(ExecutorInfo, EventKill, State) ->
    reply(ExecutorInfo, kill_task, {ExecutorInfo, EventKill}),
    {ok, State}.

acknowledged(ExecutorInfo, EventAcknowledged, State) ->
    reply(ExecutorInfo, acknowledged, {ExecutorInfo, EventAcknowledged}),
    {ok, State}.

framework_message(ExecutorInfo, EventMessage, State) ->
    reply(ExecutorInfo, framework_message, {ExecutorInfo, EventMessage}),
    {ok, State}.

error(ExecutorInfo, EventError, State) ->
    reply(ExecutorInfo, error, {EventError, State}),
    {ok, error}.

shutdown(ExecutorInfo, State) ->
    reply(ExecutorInfo, shutdown, ExecutorInfo),
    {ok, State}.

handle_info(ExecutorInfo, Info, State) ->
    reply(ExecutorInfo, handle_info, {ExecutorInfo, Info}),
    {ok, State}.

terminate(ExecutorInfo, Reason, _State) ->
    reply(ExecutorInfo, terminate, {ExecutorInfo, Reason}),
    ok.

%% Internal functions.

reply(ExecutorInfo, Name, Data) ->
    Data = term_to_binary({Name, Data}),
    erl_mesos_executor:message(ExecutorInfo, Data).

uuid() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    integer_to_binary((MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs).
