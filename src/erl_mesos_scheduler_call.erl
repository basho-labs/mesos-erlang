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
subscribe(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
          CallSubscribe) ->
    RequestOptions1 = subscribe_request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #'Call'{type = 'SUBSCRIBE', subscribe = CallSubscribe},
    Call1 = set_framework_id(SchedulerInfo, Call),
    send_request(SchedulerInfo1, Call1).

%% @doc Executes teardown call.
-spec teardown(erl_mesos_scheduler:scheduler_info()) -> ok | {error, term()}.
teardown(#scheduler_info{subscribed = false}) ->
    {error, not_subscribed};
teardown(#scheduler_info{request_options = RequestOptions} = SchedulerInfo) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #'Call'{type = 'TEARDOWN'},
    Call1 = set_framework_id(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, Call1)).

%% @doc Executes accept call.
-spec accept(erl_mesos_scheduler:scheduler_info(), erl_mesos:'Call.Accept'()) ->
    ok | {error, term()}.
accept(#scheduler_info{subscribed = false}, _CallAccept) ->
    {error, not_subscribed};
accept(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
       CallAccept) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #'Call'{type = 'ACCEPT', accept = CallAccept},
    Call1 = set_framework_id(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, Call1)).

%% @doc Executes decline call.
-spec decline(erl_mesos_scheduler:scheduler_info(),
              erl_mesos:'Call.Decline'()) ->
    ok | {error, term()}.
decline(#scheduler_info{subscribed = false}, _CallDecline) ->
    {error, not_subscribed};
decline(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
        CallDecline) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #'Call'{type = 'DECLINE', decline = CallDecline},
    Call1 = set_framework_id(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, Call1)).

%% @doc Executes revive call.
-spec revive(erl_mesos_scheduler:scheduler_info()) -> ok | {error, term()}.
revive(#scheduler_info{subscribed = false}) ->
    {error, not_subscribed};
revive(#scheduler_info{request_options = RequestOptions} = SchedulerInfo) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #'Call'{type = 'REVIVE'},
    Call1 = set_framework_id(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, Call1)).

%% @doc Executes kill call.
-spec kill(erl_mesos_scheduler:scheduler_info(), erl_mesos:'Call.Kill'()) ->
    ok | {error, term()}.
kill(#scheduler_info{subscribed = false}, _CallKill) ->
    {error, not_subscribed};
kill(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
     CallKill) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #'Call'{type = 'KILL', kill = CallKill},
    Call1 = set_framework_id(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, Call1)).

%% @doc Executes shutdown call.
-spec shutdown(erl_mesos_scheduler:scheduler_info(),
               erl_mesos:'Call.Shutdown'()) ->
     ok | {error, term()}.
shutdown(#scheduler_info{subscribed = false}, _CallShutdown) ->
    {error, not_subscribed};
shutdown(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
         CallShutdown) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #'Call'{type = 'SHUTDOWN', shutdown = CallShutdown},
    Call1 = set_framework_id(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, Call1)).

%% @doc Executes acknowledge call.
-spec acknowledge(erl_mesos_scheduler:scheduler_info(),
                  erl_mesos:'Call.Acknowledge'()) ->
    ok | {error, term()}.
acknowledge(#scheduler_info{subscribed = false}, _CallAcknowledge) ->
    {error, not_subscribed};
acknowledge(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
            CallAcknowledge) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #'Call'{type = 'ACKNOWLEDGE', acknowledge = CallAcknowledge},
    Call1 = set_framework_id(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, Call1)).

%% @doc Executes reconcile call.
-spec reconcile(erl_mesos_scheduler:scheduler_info(),
                erl_mesos:'Call.Reconcile'()) ->
    ok | {error, term()}.
reconcile(#scheduler_info{subscribed = false}, _CallReconcile) ->
    {error, not_subscribed};
reconcile(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
          CallReconcile) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #'Call'{type = 'RECONCILE', reconcile = CallReconcile},
    Call1 = set_framework_id(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, Call1)).

%% @doc Executes message call.
-spec message(erl_mesos_scheduler:scheduler_info(),
              erl_mesos:'Call.Message'()) ->
    ok | {error, term()}.
message(#scheduler_info{subscribed = false}, _CallMessage) ->
    {error, not_subscribed};
message(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
        CallMessage) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #'Call'{type = 'MESSAGE', message = CallMessage},
    Call1 = set_framework_id(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, Call1)).

%% @doc Executes request call.
-spec request(erl_mesos_scheduler:scheduler_info(), erl_mesos:'Call.Req'()) ->
    ok | {error, term()}.
request(#scheduler_info{subscribed = false}, _CallReq) ->
    {error, not_subscribed};
request(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
        CallReq) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #'Call'{type = 'REQUEST', request = CallReq},
    Call1 = set_framework_id(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, Call1)).

%% @doc Executes suppress call.
-spec suppress(erl_mesos_scheduler:scheduler_info()) -> ok | {error, term()}.
suppress(#scheduler_info{subscribed = false}) ->
    {error, not_subscribed};
suppress(#scheduler_info{request_options = RequestOptions} = SchedulerInfo) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #'Call'{type = 'SUPPRESS'},
    Call1 = set_framework_id(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, Call1)).

%% Internal functions.

%% @doc Returns subscribe request options.
%% @private
-spec subscribe_request_options(erl_mesos_http:options()) ->
    erl_mesos_http:options().
subscribe_request_options(RequestOptions) ->
    Keys = [async, recv_timeout, following_redirect],
    RequestOptions1 = [RequestOption ||
                       {Key, _Value} = RequestOption <- RequestOptions,
                       not lists:member(Key, Keys)],
    RequestOptions2 = lists:delete(async, RequestOptions1),
    [async, {recv_timeout, infinity}, {following_redirect, false} |
     RequestOptions2].

%% @doc Returns request options.
%% @private
-spec request_options(erl_mesos_http:options()) -> erl_mesos_http:options().
request_options(RequestOptions) ->
    proplists:delete(async, lists:delete(async, RequestOptions)).

%% @doc Sets framework id.
%% @private
-spec set_framework_id(erl_mesos_scheduler:scheduler_info(),
                       erl_mesos:'Call'()) ->
    erl_mesos:'Call'().
set_framework_id(#scheduler_info{framework_id = FrameworkId}, Call) ->
    Call#'Call'{framework_id = FrameworkId}.

%% @doc Sends http request.
%% @private
-spec send_request(erl_mesos_scheduler:scheduler_info(), erl_mesos:'Call'()) ->
    {ok, erl_mesos_http:client_ref()} |
    {ok, non_neg_integer(), erl_mesos_http:headers(),
     erl_mesos_http:client_ref()} |
    {error, term()}.
send_request(#scheduler_info{data_format = DataFormat,
                             api_version = ApiVersion,
                             master_host = MasterHost,
                             request_options = RequestOptions}, Call) ->
    ReqUrl = request_url(ApiVersion, MasterHost),
    ReqHeaders = request_headers(DataFormat),
    ReqBody = erl_mesos_data_format:encode(DataFormat, Call),
    erl_mesos_http:request(post, ReqUrl, ReqHeaders, ReqBody, RequestOptions).

%% @doc Returns request url.
%% @private
-spec request_url(version(), binary()) -> binary().
request_url(v1, MasterHost) ->
    <<"http://", MasterHost/binary, ?V1_API_PATH>>.

%% @doc Returns request headers.
%% @private
-spec request_headers(erl_mesos_data_format:data_format()) ->
    erl_mesos_http:headers().
request_headers(DataFormat) ->
    ContentType = erl_mesos_data_format:content_type(DataFormat),
    [{<<"Content-Type">>, ContentType},
     {<<"Accept">>, ContentType},
     {<<"Connection">>, <<"close">>}].

%% @doc Handles response.
%% @private
-spec handle_response({ok, non_neg_integer(), erl_mesos_http:headers(),
                       reference()} | {error, term()}) ->
    ok | {error, term()} |
    {error, {http_response, non_neg_integer(), binary()}}.
handle_response(Response) ->
    case Response of
        {ok, 202, _Headers, _ClientRef} ->
            ok;
        {ok, Status, _Headers, ClientRef} ->
            case erl_mesos_http:body(ClientRef) of
                {ok, Body} ->
                    {error, {http_response, Status, Body}};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.
