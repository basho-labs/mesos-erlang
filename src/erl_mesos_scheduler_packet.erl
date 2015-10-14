-module(erl_mesos_scheduler_packet).

-include("erl_mesos.hrl").

-include("erl_mesos_obj.hrl").

-export([parse/1]).

-type packet() :: {subscribed_packet, {subscribed(), pos_integer}} |
                  heartbeat_packet |
                  erl_mesos_obj:data_obj().
-export_type([packet/0]).

-define(DEFAULT_HEARTBEAT_INTERVAL_SECONDS, 15).

%% External functions.

%% @doc Parse packet.
-spec parse(erl_mesos_obj:data_obj()) -> packet().
parse(Packet) ->
    case erl_mesos_obj:get_value(<<"type">>, Packet) of
        <<"SUBSCRIBED">> ->
            {subscribed_packet, parse_subscribed(Packet)};
        <<"HEARTBEAT">> ->
            heartbeat_packet;
        _Type ->
            Packet
    end.

%% Internal functions.

%% @doc Parses subscribed obj.
%% @private
-spec parse_subscribed(erl_mesos_obj:data_obj()) ->
    {subscribed(), pos_integer()}.
parse_subscribed(Packet) ->
    SubscribedObj = erl_mesos_obj:get_value(<<"subscribed">>, Packet),
    #subscribed{framework_id = FrameworkIdObj,
                heartbeat_interval_seconds = HeartbeatIntervalSeconds} =
        ?ERL_MESOS_OBJ_TO_RECORD(subscribed, SubscribedObj),
    FrameworkId = ?ERL_MESOS_OBJ_TO_RECORD(framework_id, FrameworkIdObj),
    HeartbeatTimeout = case HeartbeatIntervalSeconds of
                           undefined ->
                               ?DEFAULT_HEARTBEAT_INTERVAL_SECONDS;
                           _HeartbeatIntervalSeconds ->
                               HeartbeatIntervalSeconds
                       end,
    {#subscribed{framework_id = FrameworkId}, HeartbeatTimeout * 1000}.
