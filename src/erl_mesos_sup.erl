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

%% @private

-module(erl_mesos_sup).

-export([start_link/0]).

-export([init/1]).

%% External functions.

%% @doc Starts the `erl_mesos_sup' process.
-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {}).

%% supervisor callback function.

%% @private
init({}) ->
    Specs = [{erl_mesos_scheduler_sup,
                  {erl_mesos_scheduler_sup, start_link, []},
                  permanent, 5000, supervisor, [erl_mesos_scheduler_sup]},
             {erl_mesos_scheduler_manager,
                  {erl_mesos_scheduler_manager, start_link, []},
                  permanent, 5000, worker, [erl_mesos_scheduler_manager]}],
    ets:new(erl_mesos_schedulers, [ordered_set, public, named_table]),
    {ok, {{one_for_one, 10, 10}, Specs}}.
