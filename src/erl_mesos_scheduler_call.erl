-module(erl_mesos_scheduler_call).

-include("erl_mesos.hrl").

-include("erl_mesos_obj.hrl").

-export([subscribe/2]).

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

%% @doc Converts framework info to obj.
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

%% @doc Converts framework info capabilitie to obj.
%% @private
-spec framework_info_capabilitie_obj(undefined | erl_mesos_obj:data_string()) ->
    undefined | erl_mesos_obj:data_obj().
framework_info_capabilitie_obj(undefined) ->
    undefined;
framework_info_capabilitie_obj(FrameworkInfoCapabilitie) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(framework_info_capabilitie,
                               FrameworkInfoCapabilitie).

%% @doc Converts labels to obj.
%% @private
-spec labels_obj(undefined | labels()) -> erl_mesos_obj:data_obj().
labels_obj(undefined) ->
    undefined;
labels_obj(#labels{labels = Labels}) ->
    LabelObjs = [?ERL_MESOS_OBJ_FROM_RECORD(label, Label) || Label <- Labels],
    ?ERL_MESOS_OBJ_FROM_RECORD(labels, #labels{labels = LabelObjs}).

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






%% %% @doc Sends subscribe request.
%% -spec subscribe(erl_mesos_data_format:data_format(), version(), binary(),
%%                 erl_mesos_http:options(), framework_info()) ->
%%     {ok, erl_mesos_http:client_ref()} | {error, term()}.
%% subscribe(DataFormat, Version, MasterHost, Options, FrameworkInfo) ->
%%     Options1 = subscribe_options(Options),
%%     FrameworkInfoObj = framework_info_obj_from_record(FrameworkInfo),
%%     SubscribeObj = erl_mesos_obj:new([{<<"framework_info">>,
%%                                        FrameworkInfoObj}]),
%%     ReqObj = erl_mesos_obj:new([{<<"type">>, <<"SUBSCRIBE">>},
%%                                 {<<"subscribe">>, SubscribeObj}]),
%%     request(DataFormat, Version, MasterHost, Options1, ReqObj).
%%
%% %% @doc Sends subscribe request.
%% -spec subscribe(erl_mesos_data_format:data_format(), version(), binary(),
%%                 erl_mesos_http:options(), framework_info(), boolean(),
%%                 framework_id()) ->
%%     {ok, erl_mesos_http:client_ref()} | {error, term()}.
%% subscribe(DataFormat, Version, MasterHost, Options, FrameworkInfo, Force,
%%           FrameworkId) ->
%%     Options1 = subscribe_options(Options),
%%     FrameworkIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(framework_id, FrameworkId),
%%     FrameworkInfo1 = FrameworkInfo#framework_info{id = FrameworkIdObj},
%%     FrameworkInfoObj = framework_info_obj_from_record(FrameworkInfo1),
%%     SubscribeObj = erl_mesos_obj:new([{<<"framework_info">>,
%%                                        FrameworkInfoObj},
%%                                       {<<"force">>, Force}]),
%%     ReqObj = erl_mesos_obj:new([{<<"type">>, <<"SUBSCRIBE">>},
%%                                 {<<"framework_id">>, FrameworkIdObj},
%%                                 {<<"subscribe">>, SubscribeObj}]),
%%     request(DataFormat, Version, MasterHost, Options1, ReqObj).
%%
%% %% @doc Sends teardown request.
%% -spec teardown(erl_mesos_data_format:data_format(), version(), binary(),
%%                erl_mesos_http:options(), framework_id()) ->
%%     ok | {error, term()}.
%% teardown(DataFormat, Version, MasterHost, Options, FrameworkId) ->
%%     FrameworkIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(framework_id, FrameworkId),
%%     ReqObj = erl_mesos_obj:new([{<<"type">>, <<"TEARDOWN">>},
%%                                 {<<"framework_id">>, FrameworkIdObj}]),
%%     convert_response(request(DataFormat, Version, MasterHost, Options, ReqObj)).
%%
%% %% Internal functions.
%%
%% %% @doc Returns subscribe options.
%% %% @private
%% -spec subscribe_req_options(erl_mesos_http:options()) ->
%%     erl_mesos_http:options().
%% subscribe_req_options(ReqOptions) ->
%%     Keys = [async, recv_timeout, following_redirect],
%%     ReqOptions1 = [ReqOption || {Key, _Value} = ReqOption <- ReqOptions,
%%                    not lists:member(Key, Keys)],
%%     [{async, true},
%%      {recv_timeout, infinity},
%%      {following_redirect, false} | ReqOptions1].
%%
%% %% @doc Sends http request to the mesos master.
%% %% @private
%% -spec request(erl_mesos_data_format:data_format(), version(), binary(),
%%               erl_mesos_http:options(), erl_mesos_obj:data_obj()) ->
%%     {ok, erl_mesos_http:client_ref()} |
%%     {ok, non_neg_integer(), erl_mesos_http:headers(),
%%      erl_mesos_http:client_ref()} |
%%     {error, term()}.
%% request(DataFormat, Version, MasterHost, Options, ReqObj) ->
%%     Url = request_url(Version, MasterHost),
%%     ReqHeaders = request_headers(DataFormat),
%%     ReqBody = erl_mesos_data_format:encode(DataFormat, ReqObj),
%%     erl_mesos_http:request(post, Url, ReqHeaders, ReqBody, Options).
%%
%% %% @doc Returns request url.
%% %% @private
%% -spec request_url(version(), binary()) -> binary().
%% request_url(v1, MasterHost) ->
%%     <<"http://", MasterHost/binary, ?V1_API_PATH>>.
%%
%% %% @doc Returns request headers.
%% %% @private
%% -spec request_headers(erl_mesos_data_format:data_format()) ->
%%     erl_mesos_http:headers().
%% request_headers(DataFormat) ->
%%     [{<<"Content-Type">>, erl_mesos_data_format:content_type(DataFormat)},
%%      {<<"Connection">>, <<"close">>}].
%%
%% %% @doc Converts response.
%% %% @private
%% -spec convert_response({ok, non_neg_integer(), erl_mesos_http:headers(),
%%                         reference()} | {error, term()}) ->
%%     ok | {error, term()} |
%%     {error, {http_response, non_neg_integer(), binary()}}.
%% convert_response(Response) ->
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
%%
%% %% @doc Converts framework info to obj.
%% %% @private
%% -spec framework_info_obj_from_record(framework_info()) ->
%%     erl_mesos_obj:data_obj().
%% framework_info_obj_from_record(#framework_info{capabilities = Capabilities,
%%                                                labels = Labels} =
%%                                FrameworkInfo) ->
%%     CapabilitiesObj = capabilities_obj(Capabilities),
%%     LabelsObj = labels_obj_from_record(Labels),
%%     FrameworkInfo1 = FrameworkInfo#framework_info{capabilities =
%%                                                       CapabilitiesObj,
%%                                                   labels = LabelsObj},
%%     ?ERL_MESOS_OBJ_FROM_RECORD(framework_info, FrameworkInfo1).
%%
%% %% @doc Converts capabilities to obj.
%% %% @private
%% -spec capabilities_obj(undefined | erl_mesos_obj:data_string()) ->
%%     undefined | erl_mesos_obj:data_obj().
%% capabilities_obj(undefined) ->
%%     undefined;
%% capabilities_obj(FrameworkInfoCapabilitie) ->
%%     ?ERL_MESOS_OBJ_FROM_RECORD(framework_info_capabilitie,
%%                                FrameworkInfoCapabilitie).
%%
%% %% @doc Converts labels to obj.
%% %% @private
%% -spec labels_obj_from_record(undefined | labels()) -> erl_mesos_obj:data_obj().
%% labels_obj_from_record(undefined) ->
%%     undefined;
%% labels_obj_from_record(#labels{labels = Labels}) ->
%%     LabelObjs = [?ERL_MESOS_OBJ_FROM_RECORD(label, Label) || Label <- Labels],
%%     ?ERL_MESOS_OBJ_FROM_RECORD(labels, #labels{labels = LabelObjs}).
