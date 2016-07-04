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

-module(erl_mesos_test_executor_sup).

-behaviour(supervisor).

-export([start_link/2]).

-export([init/1]).

%% External functions.

start_link(ExecutorOptions, Options) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE,
                          {ExecutorOptions, Options}).

%% supervisor callback function.

init({ExecutorOptions, Options}) ->
    Specs = [{erl_mesos_test_executor,
                {erl_mesos_executor, start_link, [erl_mesos_test_executor,
                                                  erl_mesos_test_executor,
                                                  ExecutorOptions,
                                                  Options]},
                transient, 5000, worker, [erl_mesos_test_executor]}],
    {ok, {{one_for_one, 1, 1}, Specs}}.
