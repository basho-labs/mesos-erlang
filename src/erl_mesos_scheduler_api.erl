-module(erl_mesos_scheduler_api).

-include("erl_mesos.hrl").

-include("erl_mesos_obj.hrl").

-export([subscribe/5,
         resubscribe/7,
         teardown/5]).

-type version() :: v1.
-export_type([version/0]).

-define(V1_API_PATH, "/api/v1/scheduler").

%% External functions.

%% @doc Sends subscribe request.
-spec subscribe(erl_mesos_data_format:data_format(), version(), binary(),
                erl_mesos_http:options(), framework_info()) ->
    {ok, erl_mesos_http:client_ref()} | {error, term()}.
subscribe(DataFormat, Version, MasterHost, Options, FrameworkInfo) ->
    FrameworkInfoObj = framework_info_obj_from_record(FrameworkInfo),
    SubscribeObj = erl_mesos_obj:new([{<<"framework_info">>,
                                       FrameworkInfoObj}]),
    ReqObj = erl_mesos_obj:new([{<<"type">>, <<"SUBSCRIBE">>},
                                {<<"subscribe">>, SubscribeObj}]),
    request(DataFormat, Version, MasterHost, Options, ReqObj).

%% @doc Sends resubscribe request.
-spec resubscribe(erl_mesos_data_format:data_format(), version(), binary(),
                  erl_mesos_http:options(), framework_info(), boolean(),
                  framework_id()) ->
    {ok, erl_mesos_http:client_ref()} | {error, term()}.
resubscribe(DataFormat, Version, MasterHost, Options, FrameworkInfo,
            Force, FrameworkId) ->
    FrameworkIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(framework_id, FrameworkId),
    FrameworkInfo1 = FrameworkInfo#framework_info{id = FrameworkIdObj},
    FrameworkInfoObj = framework_info_obj_from_record(FrameworkInfo1),
    SubscribeObj = erl_mesos_obj:new([{<<"framework_info">>,
                                       FrameworkInfoObj},
                                      {<<"force">>, Force}]),
    ReqObj = erl_mesos_obj:new([{<<"type">>, <<"SUBSCRIBE">>},
                                {<<"framework_id">>, FrameworkIdObj},
                                {<<"subscribe">>, SubscribeObj}]),
    request(DataFormat, Version, MasterHost, Options, ReqObj).

%% @doc Sends teardown request.
-spec teardown(erl_mesos_data_format:data_format(), version(), binary(),
               erl_mesos_http:options(), framework_id()) ->
    ok | {error, term()}.
teardown(DataFormat, Version, MasterHost, Options, FrameworkId) ->
    FrameworkIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(framework_id, FrameworkId),
    ReqObj = erl_mesos_obj:new([{<<"type">>, <<"TEARDOWN">>},
                                {<<"framework_id">>, FrameworkIdObj}]),
    convert_response(request(DataFormat, Version, MasterHost, Options, ReqObj)).

%% Internal functions.

%% @doc Sends http request to the mesos master.
%% @private
-spec request(erl_mesos_data_format:data_format(), version(), binary(),
              erl_mesos_http:options(), erl_mesos_obj:data_obj()) ->
    {ok, erl_mesos_http:client_ref()} |
    {ok, non_neg_integer(), erl_mesos_http:headers(),
     erl_mesos_http:client_ref()} |
    {error, term()}.
request(DataFormat, Version, MasterHost, Options, ReqObj) ->
    Url = request_url(Version, MasterHost),
    ReqHeaders = request_headers(DataFormat),
    ReqBody = erl_mesos_data_format:encode(DataFormat, ReqObj),
    erl_mesos_http:request(post, Url, ReqHeaders, ReqBody, Options).

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

%% @doc Converts response.
%% @private
-spec convert_response({ok, non_neg_integer(), erl_mesos_http:headers(),
                        reference()} | {error, term()}) ->
    ok | {error, term()} |
    {error, {http_response, non_neg_integer(), binary()}}.
convert_response(Response) ->
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

%% @doc Converts framework info to obj.
%% @private
-spec framework_info_obj_from_record(framework_info()) ->
    erl_mesos_obj:data_obj().
framework_info_obj_from_record(#framework_info{capabilities = Capabilities,
                                               labels = Labels} =
                               FrameworkInfo) ->
    CapabilitiesObj = capabilities_obj(Capabilities),
    LabelsObj = labels_obj_from_record(Labels),
    FrameworkInfo1 = FrameworkInfo#framework_info{capabilities =
                                                      CapabilitiesObj,
                                                  labels = LabelsObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(framework_info, FrameworkInfo1).

%% @doc Converts capabilities to obj.
%% @private
-spec capabilities_obj(undefined | erl_mesos_obj:data_string()) ->
    undefined | erl_mesos_obj:data_obj().
capabilities_obj(undefined) ->
    undefined;
capabilities_obj(FrameworkInfoCapabilitie) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(framework_info_capabilitie,
                               FrameworkInfoCapabilitie).

%% @doc Converts labels to obj.
%% @private
-spec labels_obj_from_record(undefined | labels()) -> erl_mesos_obj:data_obj().
labels_obj_from_record(undefined) ->
    undefined;
labels_obj_from_record(#labels{labels = Labels}) ->
    LabelObjs = [?ERL_MESOS_OBJ_FROM_RECORD(label, Label) || Label <- Labels],
    ?ERL_MESOS_OBJ_FROM_RECORD(labels, #labels{labels = LabelObjs}).
