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

-module(erl_mesos_master_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([start_master/4, stop_master/1]).

-export([init/1]).

%% External functions.

%% @doc Starts the `erl_mesos_master_sup' process.
-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {}).

%% @doc Starts `erl_mesos_master' process.
-spec start_master(term(), module(), term(),
                      erl_mesos_master:options()) ->
    {ok, pid()} | {error, term()}.
start_master(Ref, Master, MasterOptions, Options) ->
    supervisor:start_child(?MODULE, [Ref, Master, MasterOptions,
                                     Options]).

%% @doc Stops `erl_mesos_master' process.
-spec stop_master(pid()) -> ok | {error, atom()}.
stop_master(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).

%% supervisor callback function.

%% @private
-spec init(term()) ->
    {ok, {{supervisor:strategy(), 1, 10}, [supervisor:child_spec()]}}.
init({}) ->
    Spec = {undefined,
               {erl_mesos_master, start_link, []},
               temporary, 5000, worker, []},
    {ok, {{simple_one_for_one, 1, 10}, [Spec]}}.
