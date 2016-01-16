%% @private

-module(erl_mesos_scheduler_call).

-include("scheduler_info.hrl").

-include("scheduler_protobuf.hrl").

-export([subscribe/2]).

-type version() :: v1.
-export_type([version/0]).

-define(V1_API_PATH, "/api/v1/scheduler").

%% External functions.

%% @doc Executes subscribe call.
-spec subscribe(erl_mesos_scheduler:scheduler_info(),
                erl_mesos_scheduler:'Call.Subscribe()'()) ->
    {ok, erl_mesos_http:client_ref()} | {error, term()}.
subscribe(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
          CallSubscribe) ->
    RequestOptions1 = subscribe_request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #'Call'{type = 'SUBSCRIBE', subscribe = CallSubscribe},
    Call1 = set_framework_id(SchedulerInfo, Call),
    send_request(SchedulerInfo1, Call1).

%% %% @doc Executes teardown call.
%% -spec teardown(erl_mesos:scheduler_info()) -> ok | {error, term()}.
%% teardown(#scheduler_info{subscribed = false}) ->
%%     {error, not_subscribed};
%% teardown(#scheduler_info{request_options = RequestOptions} = SchedulerInfo) ->
%%     RequestOptions1 = request_options(RequestOptions),
%%     SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
%%                                                   RequestOptions1},
%%     Call = #call{type = <<"TEARDOWN">>},
%%     CallObj = call_obj(SchedulerInfo, Call),
%%     handle_response(send_request(SchedulerInfo1, CallObj)).
%%
%% %% @doc Executes accept call.
%% -spec accept(erl_mesos:scheduler_info(), erl_mesos:call_accept()) ->
%%     ok | {error, term()}.
%% accept(#scheduler_info{subscribed = false}, _CallAccept) ->
%%     {error, not_subscribed};
%% accept(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
%%        CallAccept) ->
%%     RequestOptions1 = request_options(RequestOptions),
%%     SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
%%                                                   RequestOptions1},
%%     CallAcceptObj = call_accept_obj(CallAccept),
%%     Call = #call{type = <<"ACCEPT">>, accept = CallAcceptObj},
%%     CallObj = call_obj(SchedulerInfo, Call),
%%     handle_response(send_request(SchedulerInfo1, CallObj)).
%%
%% %% @doc Executes decline call.
%% -spec decline(erl_mesos:scheduler_info(), erl_mesos:call_decline()) ->
%%     ok | {error, term()}.
%% decline(#scheduler_info{subscribed = false}, _CallDecline) ->
%%     {error, not_subscribed};
%% decline(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
%%         CallDecline) ->
%%     RequestOptions1 = request_options(RequestOptions),
%%     SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
%%                                                   RequestOptions1},
%%     CallDeclineObj = call_decline_obj(CallDecline),
%%     Call = #call{type = <<"DECLINE">>, decline = CallDeclineObj},
%%     CallObj = call_obj(SchedulerInfo, Call),
%%     handle_response(send_request(SchedulerInfo1, CallObj)).
%%
%% %% @doc Executes revive call.
%% -spec revive(erl_mesos:scheduler_info()) -> ok | {error, term()}.
%% revive(#scheduler_info{subscribed = false}) ->
%%     {error, not_subscribed};
%% revive(#scheduler_info{request_options = RequestOptions} = SchedulerInfo) ->
%%     RequestOptions1 = request_options(RequestOptions),
%%     SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
%%                                                   RequestOptions1},
%%     Call = #call{type = <<"REVIVE">>},
%%     CallObj = call_obj(SchedulerInfo, Call),
%%     handle_response(send_request(SchedulerInfo1, CallObj)).
%%
%% %% @doc Executes kill call.
%% -spec kill(erl_mesos:scheduler_info(), erl_mesos:call_kill()) ->
%%     ok | {error, term()}.
%% kill(#scheduler_info{subscribed = false}, _CallKill) ->
%%     {error, not_subscribed};
%% kill(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
%%      CallKill) ->
%%     RequestOptions1 = request_options(RequestOptions),
%%     SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
%%                                                   RequestOptions1},
%%     CallKillObj = call_kill_obj(CallKill),
%%     Call = #call{type = <<"KILL">>, kill = CallKillObj},
%%     CallObj = call_obj(SchedulerInfo, Call),
%%     handle_response(send_request(SchedulerInfo1, CallObj)).
%%
%% %% @doc Executes shutdown call.
%% -spec shutdown(erl_mesos:scheduler_info(), erl_mesos:call_shutdown()) ->
%%     ok | {error, term()}.
%% shutdown(#scheduler_info{subscribed = false}, _CallShutdown) ->
%%     {error, not_subscribed};
%% shutdown(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
%%          CallShutdown) ->
%%     RequestOptions1 = request_options(RequestOptions),
%%     SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
%%                                                   RequestOptions1},
%%     CallShutdownObj = call_shutdown_obj(CallShutdown),
%%     Call = #call{type = <<"SHUTDOWN">>, shutdown = CallShutdownObj},
%%     CallObj = call_obj(SchedulerInfo, Call),
%%     handle_response(send_request(SchedulerInfo1, CallObj)).
%%
%% %% @doc Executes acknowledge call.
%% -spec acknowledge(erl_mesos:scheduler_info(), erl_mesos:call_acknowledge()) ->
%%     ok | {error, term()}.
%% acknowledge(#scheduler_info{subscribed = false}, _CallAcknowledge) ->
%%     {error, not_subscribed};
%% acknowledge(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
%%             CallAcknowledge) ->
%%     RequestOptions1 = request_options(RequestOptions),
%%     SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
%%                                                   RequestOptions1},
%%     CallAcknowledgeObj = call_acknowledge_obj(CallAcknowledge),
%%     Call = #call{type = <<"ACKNOWLEDGE">>, acknowledge = CallAcknowledgeObj},
%%     CallObj = call_obj(SchedulerInfo, Call),
%%     handle_response(send_request(SchedulerInfo1, CallObj)).
%%
%% %% @doc Executes reconcile call.
%% -spec reconcile(erl_mesos:scheduler_info(), erl_mesos:call_reconcile()) ->
%%     ok | {error, term()}.
%% reconcile(#scheduler_info{subscribed = false}, _CallReconcile) ->
%%     {error, not_subscribed};
%% reconcile(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
%%           CallReconcile) ->
%%     RequestOptions1 = request_options(RequestOptions),
%%     SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
%%                                                   RequestOptions1},
%%     CallReconcileObj = call_reconcile_obj(CallReconcile),
%%     Call = #call{type = <<"RECONCILE">>, reconcile = CallReconcileObj},
%%     CallObj = call_obj(SchedulerInfo, Call),
%%     handle_response(send_request(SchedulerInfo1, CallObj)).
%%
%% %% @doc Executes message call.
%% -spec message(erl_mesos:scheduler_info(), erl_mesos:call_message()) ->
%%     ok | {error, term()}.
%% message(#scheduler_info{subscribed = false}, _CallMessage) ->
%%     {error, not_subscribed};
%% message(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
%%         CallMessage) ->
%%     RequestOptions1 = request_options(RequestOptions),
%%     SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
%%                                                   RequestOptions1},
%%     CallMessageObj = call_message_obj(CallMessage),
%%     Call = #call{type = <<"MESSAGE">>, message = CallMessageObj},
%%     CallObj = call_obj(SchedulerInfo, Call),
%%     handle_response(send_request(SchedulerInfo1, CallObj)).
%%
%% %% @doc Executes request call.
%% -spec request(erl_mesos:scheduler_info(), erl_mesos:call_request()) ->
%%     ok | {error, term()}.
%% request(#scheduler_info{subscribed = false}, _CallRequest) ->
%%     {error, not_subscribed};
%% request(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
%%         CallRequest) ->
%%     RequestOptions1 = request_options(RequestOptions),
%%     SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
%%                                                   RequestOptions1},
%%     CallRequestObj = call_request_obj(CallRequest),
%%     Call = #call{type = <<"REQUEST">>, request = CallRequestObj},
%%     CallObj = call_obj(SchedulerInfo, Call),
%%     handle_response(send_request(SchedulerInfo1, CallObj)).
%%
%% %% @doc Executes suppress call.
%% -spec suppress(erl_mesos:scheduler_info()) -> ok | {error, term()}.
%% suppress(#scheduler_info{subscribed = false}) ->
%%     {error, not_subscribed};
%% suppress(#scheduler_info{request_options = RequestOptions} = SchedulerInfo) ->
%%     RequestOptions1 = request_options(RequestOptions),
%%     SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
%%                                                   RequestOptions1},
%%     Call = #call{type = <<"SUPPRESS">>},
%%     CallObj = call_obj(SchedulerInfo, Call),
%%     handle_response(send_request(SchedulerInfo1, CallObj)).

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

%% %% @doc Returns request options.
%% %% @private
%% -spec request_options(erl_mesos_http:options()) -> erl_mesos_http:options().
%% request_options(RequestOptions) ->
%%     proplists:delete(async, lists:delete(async, RequestOptions)).

%% @doc Sets framework id.
%% @private
-spec set_framework_id(erl_mesos_scheduler:scheduler_info(),
                       erl_mesos_scheduler:'Call'()) ->
    erl_mesos_scheduler:'Call'().
set_framework_id(#scheduler_info{framework_id = FrameworkId}, Call) ->
    Call#'Call'{framework_id = FrameworkId}.

%% @doc Sends http request.
%% @private
-spec send_request(erl_mesos_scheduler:scheduler_info(),
                   erl_mesos_scheduler:'Call'()) ->
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

%% %% @doc Handles response.
%% %% @private
%% -spec handle_response({ok, non_neg_integer(), erl_mesos_http:headers(),
%%                       reference()} | {error, term()}) ->
%%     ok | {error, term()} |
%%     {error, {http_response, non_neg_integer(), binary()}}.
%% handle_response(Response) ->
%%     case Response of
%%         {ok, 202, _Headers, _ClientRef} ->
%%             ok;
%%         {ok, Status, _Headers, ClientRef} ->
%%             case erl_mesos_http:body(ClientRef) of
%%                 {ok, Body} ->
%%                     {error, {http_response, Status, Body}};
%%                 {error, Reason} ->
%%                     {error, Reason}
%%             end;
%%         {error, Reason} ->
%%             {error, Reason}
%%     end.
