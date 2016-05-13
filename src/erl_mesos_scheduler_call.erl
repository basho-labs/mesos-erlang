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

-module(erl_mesos_scheduler_call).

-include("scheduler_info.hrl").

-include("scheduler_protobuf.hrl").

-export([subscribe/2,
         teardown/1,
         accept/2,
         decline/2,
         revive/1,
         kill/2,
         shutdown/2,
         acknowledge/2,
         reconcile/2,
         message/2,
         request/2,
         suppress/1]).

-type version() :: v1.
-export_type([version/0]).

-define(V1_API_PATH, "/api/v1/scheduler").

%% External functions.

%% @doc Executes subscribe call.
-spec subscribe(erl_mesos_scheduler:scheduler_info(),
                erl_mesos:'Call.Subscribe'()) ->
    {ok, erl_mesos_http:client_ref()} | {error, term()}.
subscribe(SchedulerInfo, CallSubscribe) ->
    Call = #'Call'{type = 'SUBSCRIBE', subscribe = CallSubscribe},
    Call1 = set_framework_id(SchedulerInfo, Call),
    async_request(SchedulerInfo, Call1).

%% @doc Executes teardown call.
-spec teardown(erl_mesos_scheduler:scheduler_info()) -> ok | {error, term()}.
teardown(SchedulerInfo) ->
    Call = #'Call'{type = 'TEARDOWN'},
    Call1 = set_framework_id(SchedulerInfo, Call),
    sync_request(SchedulerInfo, Call1).

%% @doc Executes accept call.
-spec accept(erl_mesos_scheduler:scheduler_info(), erl_mesos:'Call.Accept'()) ->
    ok | {error, term()}.
accept(SchedulerInfo, CallAccept) ->
    Call = #'Call'{type = 'ACCEPT', accept = CallAccept},
    Call1 = set_framework_id(SchedulerInfo, Call),
    sync_request(SchedulerInfo, Call1).

%% @doc Executes decline call.
-spec decline(erl_mesos_scheduler:scheduler_info(),
              erl_mesos:'Call.Decline'()) ->
    ok | {error, term()}.
decline(SchedulerInfo, CallDecline) ->
    Call = #'Call'{type = 'DECLINE', decline = CallDecline},
    Call1 = set_framework_id(SchedulerInfo, Call),
    sync_request(SchedulerInfo, Call1).

%% @doc Executes revive call.
-spec revive(erl_mesos_scheduler:scheduler_info()) -> ok | {error, term()}.
revive(SchedulerInfo) ->
    Call = #'Call'{type = 'REVIVE'},
    Call1 = set_framework_id(SchedulerInfo, Call),
    sync_request(SchedulerInfo, Call1).

%% @doc Executes kill call.
-spec kill(erl_mesos_scheduler:scheduler_info(), erl_mesos:'Call.Kill'()) ->
    ok | {error, term()}.
kill(SchedulerInfo, CallKill) ->
    Call = #'Call'{type = 'KILL', kill = CallKill},
    Call1 = set_framework_id(SchedulerInfo, Call),
    sync_request(SchedulerInfo, Call1).

%% @doc Executes shutdown call.
-spec shutdown(erl_mesos_scheduler:scheduler_info(),
               erl_mesos:'Call.Shutdown'()) ->
     ok | {error, term()}.
shutdown(SchedulerInfo, CallShutdown) ->
    Call = #'Call'{type = 'SHUTDOWN', shutdown = CallShutdown},
    Call1 = set_framework_id(SchedulerInfo, Call),
    sync_request(SchedulerInfo, Call1).

%% @doc Executes acknowledge call.
-spec acknowledge(erl_mesos_scheduler:scheduler_info(),
                  erl_mesos:'Call.Acknowledge'()) ->
    ok | {error, term()}.
acknowledge(SchedulerInfo, CallAcknowledge) ->
    Call = #'Call'{type = 'ACKNOWLEDGE', acknowledge = CallAcknowledge},
    Call1 = set_framework_id(SchedulerInfo, Call),
    sync_request(SchedulerInfo, Call1).

%% @doc Executes reconcile call.
-spec reconcile(erl_mesos_scheduler:scheduler_info(),
                erl_mesos:'Call.Reconcile'()) ->
    ok | {error, term()}.
reconcile(SchedulerInfo, CallReconcile) ->
    Call = #'Call'{type = 'RECONCILE', reconcile = CallReconcile},
    Call1 = set_framework_id(SchedulerInfo, Call),
    sync_request(SchedulerInfo, Call1).

%% @doc Executes message call.
-spec message(erl_mesos_scheduler:scheduler_info(),
              erl_mesos:'Call.Message'()) ->
    ok | {error, term()}.
message(#scheduler_info{subscribed = false}, _CallMessage) ->
    {error, not_subscribed};
message(SchedulerInfo, CallMessage) ->
    Call = #'Call'{type = 'MESSAGE', message = CallMessage},
    Call1 = set_framework_id(SchedulerInfo, Call),
    sync_request(SchedulerInfo, Call1).

%% @doc Executes request call.
-spec request(erl_mesos_scheduler:scheduler_info(), erl_mesos:'Call.Req'()) ->
    ok | {error, term()}.
request(SchedulerInfo, CallReq) ->
    Call = #'Call'{type = 'REQUEST', request = CallReq},
    Call1 = set_framework_id(SchedulerInfo, Call),
    sync_request(SchedulerInfo, Call1).

%% @doc Executes suppress call.
-spec suppress(erl_mesos_scheduler:scheduler_info()) -> ok | {error, term()}.
suppress(SchedulerInfo) ->
    Call = #'Call'{type = 'SUPPRESS'},
    Call1 = set_framework_id(SchedulerInfo, Call),
    sync_request(SchedulerInfo, Call1).

%% Internal functions.

%% @doc Sets framework id.
%% @private
-spec set_framework_id(erl_mesos_scheduler:scheduler_info(),
                       erl_mesos:'Call'()) ->
    erl_mesos:'Call'().
set_framework_id(#scheduler_info{framework_id = FrameworkId}, Call) ->
    Call#'Call'{framework_id = FrameworkId}.

%% @doc Sends async http request.
%% @private
-spec async_request(erl_mesos_scheduler:scheduler_info(), erl_mesos:'Call'()) ->
    {ok, erl_mesos_http:client_ref()} | {error, term()}.
async_request(#scheduler_info{data_format = DataFormat,
                              api_version = ApiVersion,
                              master_host = MasterHost,
                              request_options = RequestOptions}, Call) ->
    ReqUrl = request_url(ApiVersion, MasterHost),
    erl_mesos_http:async_request(ReqUrl, DataFormat, [], Call, RequestOptions).

%% @doc Sends sync http request.
%% @private
-spec sync_request(erl_mesos_scheduler:scheduler_info(), erl_mesos:'Call'()) ->
    ok | {error, term()}.
sync_request(#scheduler_info{subscribed = false}, _Call) ->
    {error, not_subscribed};
sync_request(#scheduler_info{data_format = DataFormat,
                             api_version = ApiVersion,
                             master_host = MasterHost,
                             request_options = RequestOptions,
                             stream_id = StreamId}, Call) ->
    ReqUrl = request_url(ApiVersion, MasterHost),
    ReqHeaders = sync_request_headers(StreamId),
    Response = erl_mesos_http:sync_request(ReqUrl, DataFormat, ReqHeaders, Call,
                                           RequestOptions),
    erl_mesos_http:handle_sync_response(Response).

%% @doc Returns request url.
%% @private
-spec request_url(version(), binary()) -> binary().
request_url(v1, MasterHost) ->
    <<"http://", MasterHost/binary, ?V1_API_PATH>>.

%% @doc Sync request headers.
%% @private
-spec sync_request_headers(undefined | erl_mesos_http:headers()) ->
    erl_mesos_http:headers().
sync_request_headers(undefined) ->
    [];
sync_request_headers(StreamId) ->
    [{<<"Mesos-Stream-Id">>, StreamId}].
