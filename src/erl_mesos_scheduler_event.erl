-module(erl_mesos_scheduler_event).

-include("erl_mesos.hrl").

-include("erl_mesos_obj.hrl").

-export([parse_obj/1]).

-type event() :: {subscribed, {subscribed_event(), pos_integer}} |
                 {offers, offers_event()} |
                 {error, error_event()} |
                 heartbeat |
                 term().
-export_type([event/0]).

-define(DEFAULT_HEARTBEAT_INTERVAL_SECONDS, 15).

%% External functions.

%% @doc Parses obj.
-spec parse_obj(erl_mesos_obj:data_obj()) -> event().
parse_obj(Obj) ->
    parse_obj(erl_mesos_obj:get_value(<<"type">>, Obj), Obj).

%% Internal functions.

-spec parse_obj(erl_mesos_obj:data_string(), erl_mesos_obj:data_obj()) ->
    event().
parse_obj(<<"SUBSCRIBED">>, Obj) ->
    SubscribedObj = erl_mesos_obj:get_value(<<"subscribed">>, Obj),
    #subscribed_event{framework_id = FrameworkIdObj,
                      heartbeat_interval_seconds = HeartbeatIntervalSeconds} =
        ?ERL_MESOS_OBJ_TO_RECORD(subscribed_event, SubscribedObj),
    FrameworkId = ?ERL_MESOS_OBJ_TO_RECORD(framework_id, FrameworkIdObj),
    HeartbeatIntervalSeconds1 =
        heartbeat_interval_seconds(HeartbeatIntervalSeconds),
    SubscribedEvent = #subscribed_event{framework_id = FrameworkId,
                                        heartbeat_interval_seconds =
                                            HeartbeatIntervalSeconds1},
    {subscribed, {SubscribedEvent, HeartbeatIntervalSeconds1 * 1000}};
parse_obj(<<"OFFERS">>, Obj) ->
    _OfferObjs = erl_mesos_obj:get_value(<<"offers">>, Obj),
    OffersEvent = #offers_event{offers = [#offer{}]},
    {offers, OffersEvent};
parse_obj(<<"ERROR">>, Obj) ->
    ErrorEvent = ?ERL_MESOS_OBJ_TO_RECORD(error_event, Obj),
    {error, ErrorEvent};
parse_obj(<<"HEARTBEAT">>, _Obj) ->
    heartbeat;
parse_obj(_Type, Obj) ->
    Obj.

%% @doc Returns heartbeat interval.
%% @private
-spec heartbeat_interval_seconds(undefined | pos_integer()) -> pos_integer().
heartbeat_interval_seconds(undefined) ->
    ?DEFAULT_HEARTBEAT_INTERVAL_SECONDS;
heartbeat_interval_seconds(HeartbeatIntervalSeconds) ->
    HeartbeatIntervalSeconds.
