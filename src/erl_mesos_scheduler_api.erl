-module(erl_mesos_scheduler_api).

-include("erl_mesos.hrl").

-include("erl_mesos_obj.hrl").

-export([subscribe/5,
         resubscribe/5,
         teardown/4]).

-define(API_PATH, "/api/v1/scheduler").

%% External functions.

%% @doc Sends subscribe request.
-spec subscribe(erl_mesos_data_format:data_format(), binary(),
                erl_mesos_http:options(), framework_info(), boolean()) ->
    {ok, erl_mesos_http:client_ref()} | {error, term()}.
subscribe(DataFormat, MasterHost, Options, FrameworkInfo, Force) ->
    FrameworkInfoObj = framework_info_obj_from_record(FrameworkInfo),
    SubscribeObj = erl_mesos_obj:new([{<<"framework_info">>,
                                       FrameworkInfoObj},
                                      {<<"force">>, Force}]),
    ReqObj = erl_mesos_obj:new([{<<"type">>, <<"SUBSCRIBE">>},
                                {<<"subscribe">>, SubscribeObj}]),
    request(DataFormat, MasterHost, Options, ReqObj).

%% @doc Sends resubscribe request.
-spec resubscribe(erl_mesos_data_format:data_format(), binary(),
                  erl_mesos_http:options(), framework_info(), framework_id()) ->
    {ok, erl_mesos_http:client_ref()} | {error, term()}.
resubscribe(DataFormat, MasterHost, Options, FrameworkInfo, FrameworkId) ->
    FrameworkIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(framework_id, FrameworkId),
    FrameworkInfo1 = FrameworkInfo#framework_info{id = FrameworkIdObj},
    FrameworkInfoObj = framework_info_obj_from_record(FrameworkInfo1),
    SubscribeObj = erl_mesos_obj:new([{<<"framework_info">>,
                                       FrameworkInfoObj}]),
    ReqObj = erl_mesos_obj:new([{<<"type">>, <<"SUBSCRIBE">>},
                                {<<"framework_id">>, FrameworkIdObj},
                                {<<"subscribe">>, SubscribeObj}]),
    request(DataFormat, MasterHost, Options, ReqObj).

%% @doc Sends teardown request.
-spec teardown(erl_mesos_data_format:data_format(), binary(),
               erl_mesos_http:options(), framework_id()) ->
    ok | {error, term()}.
teardown(DataFormat, MasterHost, Options, FrameworkId) ->
    FrameworkIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(framework_id, FrameworkId),
    ReqObj = erl_mesos_obj:new([{<<"type">>, <<"TEARDOWN">>},
                                {<<"framework_id">>, FrameworkIdObj}]),
    convert_response(request(DataFormat, MasterHost, Options, ReqObj)).

%% Internal functions.

%% @doc Sends http request to the mesos master.
%% @private
-spec request(erl_mesos_data_format:data_format(), binary(),
              erl_mesos_http:options(), erl_mesos_obj:data_obj()) ->
    {ok, erl_mesos_http:client_ref()} |
    {ok, non_neg_integer(), erl_mesos_http:headers(),
     erl_mesos_http:client_ref()} |
    {error, term()}.
request(DataFormat, MasterHost, Options, ReqObj) ->
    Url = request_url(MasterHost),
    ReqHeaders = request_headers(DataFormat),
    ReqBody = erl_mesos_data_format:encode(DataFormat, ReqObj),
    erl_mesos_http:request(post, Url, ReqHeaders, ReqBody, Options).

%% @doc Returns request url.
%% @private
-spec request_url(binary()) -> binary().
request_url(MasterHost) ->
    <<"http://", MasterHost/binary, ?API_PATH>>.

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
framework_info_obj_from_record(#framework_info{labels = Labels} =
                               FrameworkInfo) ->
    LabelObjs = lebel_obj_from_record(Labels),
    FrameworkInfo1 = FrameworkInfo#framework_info{labels = LabelObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(framework_info, FrameworkInfo1).

%% @doc Converts labels info to obj.
%% @private
-spec lebel_obj_from_record(undefined | [label()]) -> erl_mesos_obj:data_obj().
lebel_obj_from_record(undefined) ->
    undefined;
lebel_obj_from_record(Labels) ->
    LebelObjs = lebel_obj_from_record(Labels, []),
    erl_mesos_obj:new([{<<"labels">>, LebelObjs}]).

%% @doc Converts labels info to objs.
%% @private
-spec lebel_obj_from_record(undefined | [label()],
                            [erl_mesos_obj:data_obj()]) ->
    [erl_mesos_obj:data_obj()].
lebel_obj_from_record([Label | Labels], LabelObjs) ->
    LabelObj = ?ERL_MESOS_OBJ_FROM_RECORD(label, Label),
    lebel_obj_from_record(Labels, [LabelObj | LabelObjs]);
lebel_obj_from_record([], LabelObjs) ->
    lists:reverse(LabelObjs).
