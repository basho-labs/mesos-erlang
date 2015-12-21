-module(erl_mesos_scheduler_call).

-include("erl_mesos.hrl").

-include("erl_mesos_obj.hrl").

-export([subscribe/2, accept/2]).

-type version() :: v1.
-export_type([version/0]).

-define(V1_API_PATH, "/api/v1/scheduler").

%% External functions.

%% Executes subscribe call.
-spec subscribe(scheduler_info(), call_subscribe()) ->
    {ok, erl_mesos_http:client_ref()} | {error, term()}.
subscribe(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
          CallSubscribe) ->
    RequestOptions1 = subscribe_request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    CallSubscribeObj = call_subscribe_obj(CallSubscribe),
    Call = #call{type = <<"SUBSCRIBE">>, subscribe = CallSubscribeObj},
    CallObj = call_obj(SchedulerInfo, Call),
    request(SchedulerInfo1, CallObj).

%% Executes accept call.
-spec accept(scheduler_info(), call_subscribe()) -> ok | {error, term()}.
accept(#scheduler_info{subscribed = false}, _CallAccept) ->
    {error, not_subscribed};
accept(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
       CallAccept) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    CallAcceptObj = call_accept_obj(CallAccept),
    Call = #call{type = <<"ACCEPT">>, accept = CallAcceptObj},
    CallObj = call_obj(SchedulerInfo, Call),
    handle_response(request(SchedulerInfo1, CallObj)).

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

%% @doc Returns call obj.
%% @private
-spec call_obj(scheduler_info(), call()) -> erl_mesos_obj:data_obj().
call_obj(#scheduler_info{framework_id = FrameworkId}, Call) ->
    FrameworkIdObj = framework_id_obj(FrameworkId),
    Call1 = Call#call{framework_id = FrameworkIdObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(call, Call1).

%% @doc Returns call subscribe obj.
%% @private
-spec call_subscribe_obj(call_subscribe()) -> erl_mesos_obj:data_obj().
call_subscribe_obj(#call_subscribe{framework_info = FrameworkInfo} =
                   CallSubscribe) ->
    FrameworkInfoObj = framework_info_obj(FrameworkInfo),
    CallSubscribe1 =
        CallSubscribe#call_subscribe{framework_info = FrameworkInfoObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(call_subscribe, CallSubscribe1).

%% @doc Returns framework info obj.
%% @private
-spec framework_info_obj(framework_info()) -> erl_mesos_obj:data_obj().
framework_info_obj(#framework_info{id = FrameworkId,
                                   capabilities = Capabilities,
                                   labels = Labels} = FrameworkInfo) ->
    FrameworkIdObj = framework_id_obj(FrameworkId),
    CapabilitiesObj = framework_info_capabilitie_obj(Capabilities),
    LabelsObj = labels_obj(Labels),
    FrameworkInfo1 = FrameworkInfo#framework_info{id = FrameworkIdObj,
                                                  capabilities =
                                                      CapabilitiesObj,
                                                  labels = LabelsObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(framework_info, FrameworkInfo1).

%% @doc Returns framework id obj.
%% @private
-spec framework_id_obj(undefined | framework_id()) -> erl_mesos_obj:data_obj().
framework_id_obj(undefined) ->
    undefined;
framework_id_obj(FrameworkId) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(framework_id, FrameworkId).

%% @doc Returns framework info capabilitie obj.
%% @private
-spec framework_info_capabilitie_obj(undefined | erl_mesos_obj:data_string()) ->
    undefined | erl_mesos_obj:data_obj().
framework_info_capabilitie_obj(undefined) ->
    undefined;
framework_info_capabilitie_obj(FrameworkInfoCapabilitie) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(framework_info_capabilitie,
                               FrameworkInfoCapabilitie).

%% @doc Returns labels obj.
%% @private
-spec labels_obj(undefined | labels()) -> erl_mesos_obj:data_obj().
labels_obj(undefined) ->
    undefined;
labels_obj(#labels{labels = Labels}) ->
    LabelObjs = [?ERL_MESOS_OBJ_FROM_RECORD(label, Label) || Label <- Labels],
    ?ERL_MESOS_OBJ_FROM_RECORD(labels, #labels{labels = LabelObjs}).

%% @doc Returns call accept obj.
%% @private
-spec call_accept_obj(call_accept()) -> erl_mesos_obj:data_obj().
call_accept_obj(#call_accept{offer_ids = OfferIds,
                             operations = OfferOperations} = CallAccept) ->
    OfferIdObjs = offer_id_objs(OfferIds),
    OfferOperationObjs = offer_operation_objs(OfferOperations),
    CallAccept1 = CallAccept#call_accept{offer_ids = OfferIdObjs,
                                         operations = OfferOperationObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(call_accept, CallAccept1).

%% @doc Returns offer id objs.
%% @private
-spec offer_id_objs(undefined | [offer_id()]) ->
    undefined | [erl_mesos_obj:data_obj()].
offer_id_objs(undefined) ->
    undefined;
offer_id_objs(OfferIds) ->
    [?ERL_MESOS_OBJ_FROM_RECORD(offer_id, OfferId) || OfferId <- OfferIds].

%% @doc Returns offer operation objs.
%% @private
-spec offer_operation_objs(undefined | [offer_operation()]) ->
    undefined | [erl_mesos_obj:data_obj()].
offer_operation_objs(undefined) ->
    undefined;
offer_operation_objs(OfferOperations) ->
    [offer_operation_obj(OfferOperation) || OfferOperation <- OfferOperations].

%% @doc Returns offer operation obj.
%% @private
-spec offer_operation_obj(offer_operation()) -> erl_mesos_obj:data_obj().
offer_operation_obj(#offer_operation{launch = OfferOperationLaunch} =
                    OfferOperation) ->
    OfferOperationLaunchObj = offer_operation_launch_obj(OfferOperationLaunch),
    OfferOperation1 =
        OfferOperation#offer_operation{launch = OfferOperationLaunchObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(offer_operation, OfferOperation1).

%% @doc Returns offer operation launch obj.
%% @private
-spec offer_operation_launch_obj(undefined | offer_operation_launch()) ->
    undefined | erl_mesos_obj:data_obj().
offer_operation_launch_obj(undefined) ->
    undefined;
offer_operation_launch_obj(#offer_operation_launch{task_infos = _TaskInfos} =
                           OfferOperationLaunch) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(offer_operation_launch, OfferOperationLaunch).

%% @doc Sends http request.
%% @private
-spec request(scheduler_info(), erl_mesos_obj:data_obj()) ->
    {ok, erl_mesos_http:client_ref()} |
    {ok, non_neg_integer(), erl_mesos_http:headers(),
     erl_mesos_http:client_ref()} |
    {error, term()}.
request(#scheduler_info{data_format = DataFormat,
                        api_version = ApiVersion,
                        master_host = MasterHost,
                        request_options = RequestOptions}, CallObj) ->
    ReqUrl = request_url(ApiVersion, MasterHost),
    ReqHeaders = request_headers(DataFormat),
    ReqBody = erl_mesos_data_format:encode(DataFormat, CallObj),
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
    [{<<"Content-Type">>, erl_mesos_data_format:content_type(DataFormat)},
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
