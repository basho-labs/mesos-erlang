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

-record(state, {task_id, disconnected_executor_info, reregister_executor_info}).

%% erl_mesos_executor callback functions.

init(_Options) ->
    {ok, #'Call.Subscribe'{}, #state{}}.

registered(ExecutorInfo, EventSubscribed, State) ->
    reply(ExecutorInfo, registered, {ExecutorInfo, EventSubscribed}),
    {ok, State}.

disconnected(ExecutorInfo, State) ->
    {ok, State#state{disconnected_executor_info = ExecutorInfo}}.

reregister(ExecutorInfo, State) ->
    reply(ExecutorInfo, reregister, ExecutorInfo),
    {ok, #'Call.Subscribe'{},
     State#state{reregister_executor_info = ExecutorInfo}}.

reregistered(ExecutorInfo, #state{disconnected_executor_info =
                                      DisconnectedExecutorInfo,
                                  reregister_executor_info =
                                      ReregisterExecutorInfo} = State) ->
    Data = {ExecutorInfo, DisconnectedExecutorInfo, ReregisterExecutorInfo},
    reply(ExecutorInfo, reregistered, Data),
    {ok, State}.

launch_task(ExecutorInfo, #'Event.Launch'{task = TaskInfo}, State) ->
    #'TaskInfo'{task_id = TaskId,
                agent_id = AgentId} = TaskInfo,
    TaskStatus = #'TaskStatus'{task_id = TaskId,
                               state = 'TASK_RUNNING',
                               source = 'SOURCE_EXECUTOR',
                               agent_id = AgentId,
                               uuid = uuid()},
    Update = erl_mesos_executor:update(ExecutorInfo, TaskStatus),
    reply(ExecutorInfo, launch_task, {Update, ExecutorInfo}),
    {ok, State#state{task_id = TaskId}}.

kill_task(ExecutorInfo, EventKill, State) ->
    reply(ExecutorInfo, kill_task, {ExecutorInfo, EventKill}),
    {ok, State}.

acknowledged(ExecutorInfo, EventAcknowledged, State) ->
    reply(ExecutorInfo, acknowledged, {ExecutorInfo, EventAcknowledged}),
    {ok, State}.

framework_message(ExecutorInfo, EventMessage, State) ->
    case EventMessage of
        #'Event.Message'{data = <<"disconnect">>} ->
            Pid = response_pid(),
            exit(Pid, kill);
        #'Event.Message'{data = <<"info">>} ->
            self() ! info;
        #'Event.Message'{data = <<"stop">>} ->
            self() ! stop;
        EventMessage ->
            reply(ExecutorInfo, framework_message, {ExecutorInfo, EventMessage})
    end,
    {ok, State}.

error(_ExecutorInfo, _EventError, State) ->
    %% See coments in proto/mesos/v1/executor/executor.proto
    {ok, State}.

shutdown(ExecutorInfo, State) ->
    reply(ExecutorInfo, shutdown, ExecutorInfo),
    {ok, State}.

handle_info(_ExecutorInfo, stop, State) ->
    {stop, State};
handle_info(ExecutorInfo, Info, State) ->
    reply(ExecutorInfo, handle_info, {ExecutorInfo, Info}),
    {ok, State}.

terminate(ExecutorInfo, Reason, _State) ->
    reply(ExecutorInfo, terminate, {ExecutorInfo, Reason}),
    ok.

%% Internal functions.

reply(ExecutorInfo, Name, Data) ->
    erl_mesos_executor:message(ExecutorInfo, term_to_binary({Name, Data})).

uuid() ->
    <<U:32, U1:16, _:4, U2:12, _:2, U3:30, U4:32>> = crypto:rand_bytes(16),
    <<U:32, U1:16, 4:4, U2:12, 2#10:2, U3:30, U4:32>>.

response_pid() ->
    [{ClientRef, _Request} | _] = ets:tab2list(hackney_manager),
    {ok, Pid} = hackney_manager:async_response_pid(ClientRef),
    Pid.
