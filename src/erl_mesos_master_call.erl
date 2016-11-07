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

-module(erl_mesos_master_call).

-include("master_info.hrl").

-include("master_protobuf.hrl").

-export([subscribe/1,
         get_version/1,
         get_state/1,
         get_agents/1,
         get_frameworks/1,
         get_executors/1,
         get_tasks/1,
         get_roles/1,
         get_master/1]).

-type version() :: v1.
-export_type([version/0]).

-define(V1_API_PATH, "/api/v1").

%% External functions.

%% @doc Executes subscribe call.
-spec subscribe(erl_mesos_master:master_info()) ->
    {ok, erl_mesos_http:client_ref()} | {error, term()}.
subscribe(MasterInfo) ->
    Call = #'Call'{type = 'SUBSCRIBE'},
    async_request(MasterInfo, Call).

%% @doc Executes GetVersion call.
-spec get_version(erl_mesos_master:master_info()) -> term() | {error, term()}.
get_version(MasterInfo) ->
    Call = #'Call'{type = 'GET_VERSION'},
    sync_request(MasterInfo, Call).

%% @doc Executes GetState call.
-spec get_state(erl_mesos_master:master_info()) -> term() | {error, term()}.
get_state(MasterInfo) ->
    Call = #'Call'{type = 'GET_STATE'},
    sync_request(MasterInfo, Call).

%% @doc Executes GetAgents call.
-spec get_agents(erl_mesos_master:master_info()) -> term() | {error, term()}.
get_agents(MasterInfo) ->
    Call = #'Call'{type = 'GET_AGENTS'},
    sync_request(MasterInfo, Call).

%% @doc Executes GetFrameworks call.
-spec get_frameworks(erl_mesos_master:master_info()) -> term() | {error, term()}.
get_frameworks(MasterInfo) ->
    Call = #'Call'{type = 'GET_FRAMEWORKS'},
    sync_request(MasterInfo, Call).

%% @doc Executes GetExecutors call.
-spec get_executors(erl_mesos_master:master_info()) -> term() | {error, term()}.
get_executors(MasterInfo) ->
    Call = #'Call'{type = 'GET_EXECUTORS'},
    sync_request(MasterInfo, Call).

%% @doc Executes GetTasks call.
-spec get_tasks(erl_mesos_master:master_info()) -> term() | {error, term()}.
get_tasks(MasterInfo) ->
    Call = #'Call'{type = 'GET_TASKS'},
    sync_request(MasterInfo, Call).

%% @doc Executes GetRoles call.
-spec get_roles(erl_mesos_master:master_info()) -> term() | {error, term()}.
get_roles(MasterInfo) ->
    Call = #'Call'{type = 'GET_ROLES'},
    sync_request(MasterInfo, Call).

%% @doc Executes GetMaster call.
-spec get_master(erl_mesos_master:master_info()) -> term() | {error, term()}.
get_master(MasterInfo) ->
    Call = #'Call'{type = 'GET_MASTER'},
    sync_request(MasterInfo, Call).

%% Internal functions.

%% @doc Sends async http request.
%% @private
-spec async_request(erl_mesos_master:master_info(),
                    erl_mesos_scheduler:'Call'()) ->
    {ok, erl_mesos_http:client_ref()} | {error, term()}.
async_request(#master_info{data_format = DataFormat,
                           data_format_module = DataFormatModule,
                           api_version = ApiVersion,
                           master_host = MasterHost,
                           request_options = RequestOptions}, Call) ->
    ReqUrl = request_url(ApiVersion, MasterHost),
    erl_mesos_http:async_request(ReqUrl, DataFormat, DataFormatModule, [], Call,
                                 RequestOptions).

%% @doc Sends sync http request.
%% @private
-spec sync_request(erl_mesos_master:master_info(),
                   erl_mesos_scheduler:'Call'()) ->
    ok | {error, term()}.
sync_request(#master_info{subscribed = false}, _Call) ->
    {error, not_subscribed};
sync_request(#master_info{data_format = DataFormat,
                          data_format_module = DataFormatModule,
                          api_version = ApiVersion,
                          master_host = MasterHost,
                          request_options = RequestOptions}, Call) ->
    ReqUrl = request_url(ApiVersion, MasterHost),
    ReqHeaders = [],
    Response = erl_mesos_http:sync_request(ReqUrl, DataFormat, DataFormatModule,
                                           ReqHeaders, Call, RequestOptions),
    erl_mesos_http:handle_sync_response(Response).

%% @doc Returns request url.
%% @private
-spec request_url(version(), binary()) -> binary().
request_url(v1, MasterHost) ->
    <<"http://", MasterHost/binary, ?V1_API_PATH>>.
