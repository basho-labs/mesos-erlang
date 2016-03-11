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

%% @private

-module(erl_mesos_scheduler_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([start_scheduler/4, stop_scheduler/1]).

-export([init/1]).

%% External functions.

%% @doc Starts the `erl_mesos_scheduler_sup' process.
-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {}).

%% @doc Starts `erl_mesos_scheduler' process.
-spec start_scheduler(term(), module(), term(),
                      erl_mesos_scheduler:options()) ->
    {ok, pid()} | {error, term()}.
start_scheduler(Ref, Scheduler, SchedulerOptions, Options) ->
    supervisor:start_child(?MODULE, [Ref, Scheduler, SchedulerOptions,
                                     Options]).

%% @doc Stops `erl_mesos_scheduler' process.
-spec stop_scheduler(pid()) -> ok | {error, atom()}.
stop_scheduler(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).

%% supervisor callback function.

%% @private
init({}) ->
    Spec = {undefined,
               {erl_mesos_scheduler, start_link, []},
               temporary, 5000, worker, []},
    {ok, {{simple_one_for_one, 1, 10}, [Spec]}}.
