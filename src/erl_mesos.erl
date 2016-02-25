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
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(erl_mesos).

-behaviour(application).

-include("scheduler_protobuf.hrl").

-export([start/0]).

-export([start_scheduler/4, start_scheduler/5, stop_scheduler/1]).

-export([start/2, stop/1]).

-type 'AgentID'() :: #'AgentID'{}.
-export_type(['AgentID'/0]).

-type 'Call'() :: #'Call'{}.
-export_type(['Call'/0]).

-type 'Call.Subscribe'() :: #'Call.Subscribe'{}.
-export_type(['Call.Subscribe'/0]).

-type 'Call.Accept'() :: #'Call.Accept'{}.
-export_type(['Call.Accept'/0]).

-type 'Call.Decline'() :: #'Call.Decline'{}.
-export_type(['Call.Decline'/0]).

-type 'Call.Kill'() :: #'Call.Kill'{}.
-export_type(['Call.Kill'/0]).

-type 'Call.Shutdown'() :: #'Call.Shutdown'{}.
-export_type(['Call.Shutdown'/0]).

-type 'Call.Acknowledge'() :: #'Call.Acknowledge'{}.
-export_type(['Call.Acknowledge'/0]).

-type 'Call.Reconcile.Task'() :: #'Call.Reconcile.Task'{}.
-export_type(['Call.Reconcile.Task'/0]).

-type 'Call.Reconcile'() :: #'Call.Reconcile'{}.
-export_type(['Call.Reconcile'/0]).

-type 'Call.Message'() :: #'Call.Message'{}.
-export_type(['Call.Message'/0]).

-type 'Call.Req'() :: #'Call.Req'{}.
-export_type(['Call.Req'/0]).

-type 'CommandInfo'() :: #'CommandInfo'{}.
-export_type(['CommandInfo'/0]).

-type 'CommandInfo.URI'() :: #'CommandInfo.URI'{}.
-export_type(['CommandInfo.URI'/0]).

-type 'Event'() :: #'Event'{}.
-export_type(['Event'/0]).

-type 'Event.Subscribed'() :: #'Event.Subscribed'{}.
-export_type(['Event.Subscribed'/0]).

-type 'Event.Offers'() :: #'Event.Offers'{}.
-export_type(['Event.Offers'/0]).

-type 'Event.Rescind'() :: #'Event.Rescind'{}.
-export_type(['Event.Rescind'/0]).

-type 'Event.Update'() :: #'Event.Update'{}.
-export_type(['Event.Update'/0]).

-type 'Event.Message'() :: #'Event.Message'{}.
-export_type(['Event.Message'/0]).

-type 'Event.Failure'() :: #'Event.Failure'{}.
-export_type(['Event.Failure'/0]).

-type 'Event.Error'() :: #'Event.Error'{}.
-export_type(['Event.Error'/0]).

-type 'ExecutorID'() :: #'ExecutorID'{}.
-export_type(['ExecutorID'/0]).

-type 'ExecutorInfo'() :: #'ExecutorInfo'{}.
-export_type(['ExecutorInfo'/0]).

-type 'Filters'() :: #'Filters'{}.
-export_type(['Filters'/0]).

-type 'FrameworkID'() :: #'FrameworkID'{}.
-export_type(['FrameworkID'/0]).

-type 'FrameworkInfo'() :: #'FrameworkInfo'{}.
-export_type(['FrameworkInfo'/0]).

-type 'MasterInfo'() :: #'MasterInfo'{}.
-export_type(['MasterInfo'/0]).

-type 'Offer'() :: #'Offer'{}.
-export_type(['Offer'/0]).

-type 'Offer.Operation'() :: #'Offer.Operation'{}.
-export_type(['Offer.Operation'/0]).

-type 'OfferID'() :: #'OfferID'{}.
-export_type(['OfferID'/0]).

-type 'Resource'() :: #'Resource'{}.
-export_type(['Resource'/0]).

-type 'Request'() :: #'Request'{}.
-export_type(['Request'/0]).

-type 'TaskID'() :: #'TaskID'{}.
-export_type(['TaskID'/0]).

-type 'TaskInfo'() :: #'TaskInfo'{}.
-export_type(['TaskInfo'/0]).

-type 'TaskStatus'() :: #'TaskStatus'{}.
-export_type(['TaskStatus'/0]).

-type 'Value'() :: #'Value'{}.
-export_type(['Value'/0]).

-type 'Value.Scalar'() :: #'Value.Scalar'{}.
-export_type(['Value.Scalar'/0]).

-type 'Value.Ranges'() :: #'Value.Ranges'{}.
-export_type(['Value.Ranges'/0]).

-type 'Value.Range'() :: #'Value.Range'{}.
-export_type(['Value.Range'/0]).

-type 'Value.Set'() :: #'Value.Set'{}.
-export_type(['Value.Set'/0]).

-type 'Value.Text'() :: #'Value.Text'{}.
-export_type(['Value.Text'/0]).

%% External functions.

%% @doc Starts app with deps.
-spec start() -> ok.
start() ->
    ok = application:ensure_started(asn1),
    ok = application:ensure_started(crypto),
    ok = application:ensure_started(public_key),
    ok = application:ensure_started(ssl),
    ok = application:ensure_started(idna),
    ok = application:ensure_started(hackney),
    ok = application:ensure_started(erl_mesos).

%% @equiv erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options,
%%                                  infinity)
-spec start_scheduler(term(), module(), term(),
                      erl_mesos_scheduler:options()) ->
    {ok, pid()} | {error, term()}.
start_scheduler(Ref, Scheduler, SchedulerOptions, Options) ->
    start_scheduler(Ref, Scheduler, SchedulerOptions, Options, infinity).

%% @doc Starts scheduler.
-spec start_scheduler(term(), module(), term(),
                      erl_mesos_scheduler:options(), timeout()) ->
    {ok, pid()} | {error, term()}.
start_scheduler(Ref, Scheduler, SchedulerOptions, Options, Timeout) ->
    erl_mesos_scheduler_manager:start_scheduler(Ref, Scheduler,
                                                SchedulerOptions, Options,
                                                Timeout).

%% @doc Stops scheduler.
-spec stop_scheduler(term())  -> ok | {error, term()}.
stop_scheduler(Ref) ->
    erl_mesos_scheduler_manager:stop_scheduler(Ref).

%% application callback functions.

%% @doc Starts the `erl_mesos_sup' process.
%% @private
-spec start(normal | {takeover, node()} | {failover, node()}, term()) ->
    {ok, pid()} | {error, term()}.
start(_Type, _Args) ->
    erl_mesos_sup:start_link().

%% @private
-spec stop(term()) -> ok.
stop(_State) ->
    ok.
