-module(erl_mesos_scheduler_packet).

-include("erl_mesos.hrl").

-include("erl_mesos_obj.hrl").

-export([parse/1]).

-type packet() :: {subscribed, {subscribed_packet(), pos_integer}} |
                  {error, error_packet()} |
                  heartbeat |
                  erl_mesos_obj:data_obj().
-export_type([packet/0]).

-define(DEFAULT_HEARTBEAT_INTERVAL_SECONDS, 15).

%% External functions.

%% @doc Parses obj.
-spec parse(erl_mesos_obj:data_obj()) -> packet().
parse(Obj) ->
    case erl_mesos_obj:get_value(<<"type">>, Obj) of
        <<"SUBSCRIBED">> ->
            {subscribed, parse_subscribed_obj(Obj)};
        <<"ERROR">> ->
            {error, parse_error_obj(Obj)};
        <<"HEARTBEAT">> ->
            heartbeat;
        _Type ->
            Obj
    end.

%% Internal functions.

%% @doc Parses subscribed obj.
%% @private
-spec parse_subscribed_obj(erl_mesos_obj:data_obj()) ->
    {subscribed_packet(), pos_integer()}.
parse_subscribed_obj(Obj) ->
    SubscribedObj = erl_mesos_obj:get_value(<<"subscribed">>, Obj),
    #subscribed_packet{framework_id = FrameworkIdObj,
                       heartbeat_interval_seconds = HeartbeatIntervalSeconds} =
        ?ERL_MESOS_OBJ_TO_RECORD(subscribed_packet, SubscribedObj),
    FrameworkId = ?ERL_MESOS_OBJ_TO_RECORD(framework_id, FrameworkIdObj),
    HeartbeatTimeout = case HeartbeatIntervalSeconds of
                           undefined ->
                               ?DEFAULT_HEARTBEAT_INTERVAL_SECONDS;
                           _HeartbeatIntervalSeconds ->
                               HeartbeatIntervalSeconds
                       end,
    {#subscribed_packet{framework_id = FrameworkId}, HeartbeatTimeout * 1000}.

%% @doc Parses error obj.
%% @private
-spec parse_error_obj(erl_mesos_obj:data_obj()) -> error_packet().
parse_error_obj(Obj) ->
    ?ERL_MESOS_OBJ_TO_RECORD(error_packet, Obj).
