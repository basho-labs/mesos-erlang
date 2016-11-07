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

-module(erl_mesos_agent_call).

-include("agent_info.hrl").

-include("master_protobuf.hrl").

-export([get_version/1,
         get_state/1,
         get_frameworks/1,
         get_executors/1,
         get_tasks/1,
         get_containers/1]).

-type version() :: v1.
-export_type([version/0]).

-define(V1_API_PATH, "/api/v1").

%% External functions.

%% @doc Executes GetVersion call.
-spec get_version(erl_mesos_agent:agent_info()) -> term() | {error, term()}.
get_version(AgentInfo) ->
    Call = #'Call'{type = 'GET_VERSION'},
    sync_request(AgentInfo, Call).

%% @doc Executes GetState call.
-spec get_state(erl_mesos_agent:agent_info()) -> term() | {error, term()}.
get_state(AgentInfo) ->
    Call = #'Call'{type = 'GET_STATE'},
    sync_request(AgentInfo, Call).

%% @doc Executes GetFrameworks call.
-spec get_frameworks(erl_mesos_agent:agent_info()) -> term() | {error, term()}.
get_frameworks(AgentInfo) ->
    Call = #'Call'{type = 'GET_FRAMEWORKS'},
    sync_request(AgentInfo, Call).

%% @doc Executes GetExecutors call.
-spec get_executors(erl_mesos_agent:agent_info()) -> term() | {error, term()}.
get_executors(AgentInfo) ->
    Call = #'Call'{type = 'GET_EXECUTORS'},
    sync_request(AgentInfo, Call).

%% @doc Executes GetTasks call.
-spec get_tasks(erl_mesos_agent:agent_info()) -> term() | {error, term()}.
get_tasks(AgentInfo) ->
    Call = #'Call'{type = 'GET_TASKS'},
    sync_request(AgentInfo, Call).

%% @doc Executes GetContainers call.
-spec get_containers(erl_mesos_agent:agent_info()) -> term() | {error, term()}.
get_containers(AgentInfo) ->
    Call = #'Call'{type = 'GET_CONTAINERS'},
    sync_request(AgentInfo, Call).

%% Internal functions.

%% @doc Sends sync http request.
%% @private
-spec sync_request(erl_mesos_agent:agent_info(),
                   erl_mesos_scheduler:'Call'()) ->
    ok | {error, term()}.
sync_request(#agent_info{data_format = DataFormat,
                          data_format_module = DataFormatModule,
                          api_version = ApiVersion,
                          agent_host = AgentHost,
                          request_options = RequestOptions}, Call) ->
    ReqUrl = request_url(ApiVersion, AgentHost),
    ReqHeaders = [],
    Response = erl_mesos_http:sync_request(ReqUrl, DataFormat, DataFormatModule,
                                           ReqHeaders, Call, RequestOptions),
    erl_mesos_http:handle_sync_response(Response).

%% @doc Returns request url.
%% @private
-spec request_url(version(), binary()) -> binary().
request_url(v1, AgentHost) ->
    <<"http://", AgentHost/binary, ?V1_API_PATH>>.
