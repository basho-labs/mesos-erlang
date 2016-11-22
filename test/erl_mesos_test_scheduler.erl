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

-module(erl_mesos_test_scheduler).

-behaviour(erl_mesos_scheduler).

-include_lib("erl_mesos_scheduler_info.hrl").

-include_lib("erl_mesos_scheduler_proto.hrl").

-export([init/1,
         registered/3,
         reregistered/2,
         disconnected/2,
         resource_offers/3,
         resource_inverse_offers/3,
         offer_rescinded/3,
         inverse_offer_rescinded/3,
         status_update/3,
         framework_message/3,
         slave_lost/3,
         executor_lost/3,
         error/3,
         handle_info/3,
         terminate/2]).

-record(state, {user, test_pid}).

%% erl_mesos_scheduler callback functions.

init(Options) ->
    FrameworkInfo = framework_info(Options),
    TestPid = proplists:get_value(test_pid, Options),
    {ok, FrameworkInfo, #state{user = FrameworkInfo#'FrameworkInfo'.user,
                               test_pid = TestPid}}.

registered(SchedulerInfo, EventSubscribed,
           #state{test_pid = TestPid} = State) ->
    reply(TestPid, registered, {self(), SchedulerInfo, EventSubscribed}),
    {ok, State}.

disconnected(SchedulerInfo, #state{test_pid = TestPid} = State) ->
    reply(TestPid, disconnected, {self(), SchedulerInfo}),
    {ok, State}.

reregistered(SchedulerInfo, #state{test_pid = TestPid} = State) ->
    reply(TestPid, reregistered, {self(), SchedulerInfo}),
    {ok, State}.

resource_offers(SchedulerInfo, EventOffers,
                #state{test_pid = TestPid} = State) ->
    reply(TestPid, resource_offers, {self(), SchedulerInfo, EventOffers}),
    {ok, State}.

resource_inverse_offers(SchedulerInfo, EventInverseOffers,
                        #state{test_pid = TestPid} = State) ->
    reply(TestPid, resource_inverse_offers,
          {self(), SchedulerInfo, EventInverseOffers}),
    {ok, State}.

offer_rescinded(SchedulerInfo, EventRescind,
                #state{test_pid = TestPid} = State) ->
    reply(TestPid, offer_rescinded, {self(), SchedulerInfo, EventRescind}),
    {ok, State}.

inverse_offer_rescinded(SchedulerInfo, EventRescindInverseOffer,
                        #state{test_pid = TestPid} = State) ->
    reply(TestPid, inverse_offer_rescinded,
          {self(), SchedulerInfo, EventRescindInverseOffer}),
    {ok, State}.

status_update(SchedulerInfo, EventUpdate, #state{test_pid = TestPid} = State) ->
    reply(TestPid, status_update, {self(), SchedulerInfo, EventUpdate}),
    {ok, State}.

framework_message(SchedulerInfo, EventMessage,
                  #state{test_pid = TestPid} = State) ->
    reply(TestPid, framework_message, {self(), SchedulerInfo, EventMessage}),
    {ok, State}.

slave_lost(SchedulerInfo, EventFailure, #state{test_pid = TestPid} = State) ->
    reply(TestPid, slave_lost, {self(), SchedulerInfo, EventFailure}),
    {ok, State}.

executor_lost(SchedulerInfo, EventFailure,
              #state{test_pid = TestPid} = State) ->
    reply(TestPid, executor_lost, {self(), SchedulerInfo, EventFailure}),
    {ok, State}.

error(SchedulerInfo, EventError, #state{test_pid = TestPid} = State) ->
    reply(TestPid, error, {self(), SchedulerInfo, EventError}),
    {stop, State}.

handle_info(SchedulerInfo, teardown, #state{test_pid = TestPid} = State) ->
    Teardown = erl_mesos_scheduler:teardown(SchedulerInfo),
    reply(TestPid, teardown, Teardown),
    {stop, State};

handle_info(#scheduler_info{framework_id = FrameworkId} = SchedulerInfo,
            {accept, OfferId, AgentId, TaskId},
            #state{user = User, test_pid = TestPid} = State) ->
    CommandInfoUris =
        [erl_mesos_utils:command_info_uri("erl_mesos_test_executor.sh"),
         erl_mesos_utils:command_info_uri("erl_mesos_test_executor.tar.gz",
                                          false, true)],
    CommandInfo = erl_mesos_utils:command_info("./erl_mesos_test_executor.sh",
                                               CommandInfoUris, true, User),
    ExecutorResourceCpus = erl_mesos_utils:scalar_resource("cpus", 0.1),
    ExecutorId = erl_mesos_utils:executor_id(TaskId#'TaskID'.value),
    ExecutorInfo = erl_mesos_utils:executor_info(ExecutorId, CommandInfo,
                                                 [ExecutorResourceCpus],
                                                 FrameworkId),
    TaskResourceCpu = erl_mesos_utils:scalar_resource("cpus", 0.1),
    TaskInfo = erl_mesos_utils:task_info("erl_mesos_test_executor", TaskId,
                                         AgentId, [TaskResourceCpu],
                                         ExecutorInfo),
    OfferOperation = erl_mesos_utils:launch_offer_operation([TaskInfo]),
    Accept = erl_mesos_scheduler:accept(SchedulerInfo, [OfferId],
                                        [OfferOperation]),
    reply(TestPid, accept, Accept),
    {ok, State};
handle_info(SchedulerInfo, {decline, TaskId},
            #state{test_pid = TestPid} = State) ->
    Decline = erl_mesos_scheduler:decline(SchedulerInfo, [TaskId]),
    reply(TestPid, decline, Decline),
    {ok, State};
handle_info(SchedulerInfo, revive, #state{test_pid = TestPid} = State) ->
    Revive = erl_mesos_scheduler:revive(SchedulerInfo),
    reply(TestPid, revive, Revive),
    {ok, State};
handle_info(SchedulerInfo, {kill, TaskId},
            #state{test_pid = TestPid} = State) ->
    Kill = erl_mesos_scheduler:kill(SchedulerInfo, TaskId),
    reply(TestPid, kill, Kill),
    {ok, State};
handle_info(SchedulerInfo, {shutdown, ExecutorId, AgentId},
            #state{test_pid = TestPid} = State) ->
    Shutdown = erl_mesos_scheduler:shutdown(SchedulerInfo, ExecutorId, AgentId),
    reply(TestPid, shutdown, Shutdown),
    {ok, State};
handle_info(SchedulerInfo, {acknowledge, AgentId, TaskId, Uuid},
            #state{test_pid = TestPid} = State) ->
    Acknowledge =
        erl_mesos_scheduler:acknowledge(SchedulerInfo, AgentId, TaskId, Uuid),
    reply(TestPid, acknowledge, Acknowledge),
    {ok, State};
handle_info(SchedulerInfo, {reconcile, TaskId},
            #state{test_pid = TestPid} = State) ->
    CallReconcileTask = #'Call.Reconcile.Task'{task_id = TaskId},
    Reconcile = erl_mesos_scheduler:reconcile(SchedulerInfo,
                                              [CallReconcileTask]),
    reply(TestPid, reconcile, Reconcile),
    {ok, State};
handle_info(SchedulerInfo, {message, AgentId, ExecutorId, Data},
            #state{test_pid = TestPid} = State) ->
    Message =
        erl_mesos_scheduler:message(SchedulerInfo, AgentId, ExecutorId, Data),
    reply(TestPid, message, Message),
    {ok, State};
handle_info(SchedulerInfo, {request, Requests},
            #state{test_pid = TestPid} = State) ->
    Request = erl_mesos_scheduler:request(SchedulerInfo, Requests),
    reply(TestPid, request, Request),
    {ok, State};
handle_info(SchedulerInfo, suppress,
            #state{test_pid = TestPid} = State) ->
    Suppress = erl_mesos_scheduler:suppress(SchedulerInfo),
    reply(TestPid, suppress, Suppress),
    {ok, State};
handle_info(SchedulerInfo, {disconnect_executor, AgentId, ExecutorId}, State) ->
    ok = erl_mesos_scheduler:message(SchedulerInfo, AgentId, ExecutorId,
                                     <<"disconnect">>),
    {ok, State};
handle_info(SchedulerInfo, {info_executor, AgentId, ExecutorId}, State) ->
    ok = erl_mesos_scheduler:message(SchedulerInfo, AgentId, ExecutorId,
                                     <<"info">>),
    {ok, State};
handle_info(SchedulerInfo, {stop_executor, AgentId, ExecutorId}, State) ->
    ok = erl_mesos_scheduler:message(SchedulerInfo, AgentId, ExecutorId,
                                     <<"stop">>),
    {ok, State};
handle_info(_SchedulerInfo, stop, State) ->
    {stop, State};
handle_info(_SchedulerInfo, _Info, State) ->
    {ok, State}.

terminate(Reason, #state{test_pid = TestPid} = State) ->
    reply(TestPid, terminate, {self(), Reason, State}).

%% Internal functions.

framework_info(Options) ->
    Name = proplists:get_value(name, Options, ""),
    User = proplists:get_value(user, Options, ""),
    FailoverTimeout = proplists:get_value(failover_timeout, Options, 0.0),
    Checkpoint = proplists:get_value(checkpoint, Options, true),
    #'FrameworkInfo'{name = Name,
                     user = User,
                     failover_timeout = FailoverTimeout,
                     checkpoint = Checkpoint}.

reply(undefined, _Name, _Message) ->
    undefined;
reply(TestPid, Name, Data) ->
    TestPid ! {Name, Data}.
