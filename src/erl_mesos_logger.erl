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
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @private

-module(erl_mesos_logger).

-export([info/2, warning/2, error/2]).

%% External functions.

%% @doc Logs info.
-spec info(string(), list()) -> ok.
info(Format, Data) ->
    error_logger:info_msg(Format, Data).

%% @doc Logs warning.
-spec warning(string(), list()) -> ok.
warning(Format, Data) ->
    error_logger:warning_msg(Format, Data).

%% @doc Logs error.
-spec error(string(), list()) -> ok.
error(Format, Data) ->
    error_logger:error_msg(Format, Data).
