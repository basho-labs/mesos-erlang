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

-module(erl_mesos).

-include("erl_mesos_scheduler_proto.hrl").

-export([start_scheduler/4, stop_scheduler/1, stop_scheduler/2]).

-type 'AgentID'() :: #'AgentID'{}.
-export_type(['AgentID'/0]).

-type 'CommandInfo'() :: #'CommandInfo'{}.
-export_type(['CommandInfo'/0]).

-type 'CommandInfo.URI'() :: #'CommandInfo.URI'{}.
-export_type(['CommandInfo.URI'/0]).

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

%% @equiv erl_mesos_scheduler:start_link(Name, Scheduler, SchedulerOptions,
%%                                       Options)
-spec start_scheduler(atom(), module(), term(),
                      erl_mesos_scheduler:options()) ->
    {ok, pid()} | {error, term()}.
start_scheduler(Name, Scheduler, SchedulerOptions, Options) ->
    erl_mesos_scheduler:start_link(Name, Scheduler, SchedulerOptions, Options).

%% @equiv stop_scheduler(Name, infinity)
-spec stop_scheduler(term())  -> ok | {error, term()}.
stop_scheduler(Name) ->
    erl_mesos_scheduler:stop(Name, infinity).

%% @equiv erl_mesos_scheduler:stop(Name, Timeout)
-spec stop_scheduler(term(), timeout())  -> ok.
stop_scheduler(Name, Timeout) ->
    erl_mesos_scheduler:stop(Name, Timeout).
