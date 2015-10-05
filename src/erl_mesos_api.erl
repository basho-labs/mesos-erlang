-module(erl_mesos_api).

-include("erl_mesos.hrl").

-include("erl_mesos_obj.hrl").

-export([subscribe/5, resubscribe/5]).

-type request_options() :: [{atom(), term()}].
-export_type([request_options/0]).

-define(PATH, "/api/v1/scheduler").

%% External functions.

%% @doc Sends subscribe request.
-spec subscribe(erl_mesos_data_format:data_format(), binary(),
                request_options(), framework_info(), boolean()) ->
    {ok, hackney:client_ref()} | {error, term()}.
subscribe(DataFormat, MasterHost, Options, FrameworkInfo, Force) ->
    FrameworkInfoObj = ?ERL_MESOS_OBJ_FROM_RECORD(framework_info,
                                                  FrameworkInfo),
    SubscribeObj = erl_mesos_obj:new([{<<"framework_info">>,
                                       FrameworkInfoObj},
                                      {<<"force">>, Force}]),
    ReqObj = erl_mesos_obj:new([{<<"type">>, <<"SUBSCRIBE">>},
                                {<<"subscribe">>, SubscribeObj}]),
    request(DataFormat, MasterHost, Options, ReqObj).

%% @doc Sends resubscribe request.
-spec resubscribe(erl_mesos_data_format:data_format(), binary(),
                  request_options(), framework_info(), framework_id()) ->
    {ok, hackney:client_ref()} | {error, term()}.
resubscribe(DataFormat, MasterHost, Options, FrameworkInfo, FrameworkId) ->
    FrameworkIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(framework_id, FrameworkId),
    FrameworkInfo1 = FrameworkInfo#framework_info{id = FrameworkIdObj},
    FrameworkInfoObj = ?ERL_MESOS_OBJ_FROM_RECORD(framework_info,
                                                  FrameworkInfo1),
    SubscribeObj = erl_mesos_obj:new([{<<"framework_info">>,
                                       FrameworkInfoObj}]),
    ReqObj = erl_mesos_obj:new([{<<"type">>, <<"SUBSCRIBE">>},
                                {<<"framework_id">>, FrameworkIdObj},
                                {<<"subscribe">>, SubscribeObj}]),
    request(DataFormat, MasterHost, Options, ReqObj).

%% Internal functions.

%% @doc Sends http request to the mesos master.
%% @private
-spec request(erl_mesos_data_format:data_format(), binary(), request_options(),
              erl_mesos_obj:data_obj()) ->
    {ok, hackney:client_ref()} | {error, term()}.
request(DataFormat, MasterHost, Options, ReqObj) ->
    Url = <<"http://", MasterHost/binary, ?PATH>>,
    ReqHeaders = [{<<"Content-Type">>,
                  erl_mesos_data_format:content_type(DataFormat)}],
    ReqBody = erl_mesos_data_format:encode(DataFormat, ReqObj),
    hackney:request(post, Url, ReqHeaders, ReqBody, Options).
