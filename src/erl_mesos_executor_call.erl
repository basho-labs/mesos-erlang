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

-module(erl_mesos_executor_call).

-include("executor_info.hrl").

-include("executor_protobuf.hrl").

-export([subscribe/2]).

-type version() :: v1.
-export_type([version/0]).

-define(V1_API_PATH, "/api/v1/executor").

%% External functions.

%% @doc Executes subscribe call.
-spec subscribe(erl_mesos_executor:executor_info(),
                erl_mesos_executor:'Call.Subscribe'()) ->
    {ok, erl_mesos_http:client_ref()} | {error, term()}.
subscribe(ExecutorInfo, CallSubscribe) ->
    Call = #'Call'{type = 'SUBSCRIBE', subscribe = CallSubscribe},
    Call1 = set_ids(ExecutorInfo, Call),
    async_request(ExecutorInfo, Call1).

%% Internal functions.

%% @doc Sets executor id and framework id.
%% @private
-spec set_ids(erl_mesos_executor:executor_info(),
              erl_mesos_executor:'Call'()) ->
    erl_mesos_executor:'Call'().
set_ids(#executor_info{executor_id = ExecutorId,
                       framework_id = FrameworkId}, Call) ->
    Call#'Call'{executor_id = ExecutorId, framework_id = FrameworkId}.

%% @doc Sends async http request.
%% @private
-spec async_request(erl_mesos_executor:executor_info(),
                    erl_mesos_executor:'Call'()) ->
    {ok, erl_mesos_http:client_ref()} | {error, term()}.
async_request(#executor_info{data_format = DataFormat,
                             api_version = ApiVersion,
                             agent_host = AgentHost,
                             request_options = RequestOptions}, Call) ->
    ReqUrl = request_url(ApiVersion, AgentHost),
    erl_mesos_http:async_request(ReqUrl, DataFormat, [], Call, RequestOptions).

%% @doc Returns request url.
%% @private
-spec request_url(version(), binary()) -> binary().
request_url(v1, AgentHost) ->
    <<"http://", AgentHost/binary, ?V1_API_PATH>>.
