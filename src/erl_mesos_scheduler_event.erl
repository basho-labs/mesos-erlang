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
    Obj1 = erl_mesos_obj:get_value(<<"offers">>, Obj),
    OfferObjs = erl_mesos_obj:get_value(<<"offers">>, Obj1),
    io:format("OfferObjs ~p~n", [OfferObjs]),

    parse_offer_objs(OfferObjs, []),

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

%% @doc Parses offer objs.
%% @private
-spec parse_offer_objs([erl_mesos_obj:data_obj()], [offer()]) -> [offer()].
parse_offer_objs([OfferObj | OfferObjs], Offers) ->
    Offer = ?ERL_MESOS_OBJ_TO_RECORD(offer, OfferObj),
    Id = ?ERL_MESOS_OBJ_TO_RECORD(offer_id, Offer#offer.id),
    FrameworkId = ?ERL_MESOS_OBJ_TO_RECORD(framework_id,
                                           Offer#offer.framework_id),
    AgentId = ?ERL_MESOS_OBJ_TO_RECORD(agent_id, Offer#offer.agent_id),
    Resources = parse_resource_objs(Offer#offer.resources, []),
    Offer1 = Offer#offer{id = Id,
                         framework_id = FrameworkId,
                         agent_id = AgentId,
                         resources = Resources},
    io:format("Offer ~p~n", [Offer1]),
    parse_offer_objs(OfferObjs, [Offer1 | Offers]);
parse_offer_objs([], Offers) ->
    lists:reverse(Offers).

%% @doc Parses resource objs.
%% @private
-spec parse_resource_objs([erl_mesos_obj:data_obj()], [resource()]) ->
    undefined | [resource()].
parse_resource_objs([ResourceObj | ResourceObjs], Resources) ->
    Resource = ?ERL_MESOS_OBJ_TO_RECORD(resource, ResourceObj),
    Scalar = parse_value_scalar_obj(Resource#resource.scalar),
    Ranges = parse_value_ranges_obj(Resource#resource.ranges),
    Set = parse_value_set_obj(Resource#resource.set),
    Resource1 = Resource#resource{scalar = Scalar,
                                  ranges = Ranges,
                                  set = Set},
    parse_resource_objs(ResourceObjs, [Resource1 | Resources]);
parse_resource_objs([], Resources) ->
    lists:reverse(Resources).

%% @doc Parses value scalar obj.
%% @private
-spec parse_value_scalar_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | value_scalar().
parse_value_scalar_obj(undefined) ->
    undefined;
parse_value_scalar_obj(ValueScalarObj) ->
    ?ERL_MESOS_OBJ_TO_RECORD(value_scalar, ValueScalarObj).

%% @doc Parses value ranges obj.
%% @private
-spec parse_value_ranges_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | value_ranges().
parse_value_ranges_obj(undefined) ->
    undefined;
parse_value_ranges_obj(ValueRangesObj) ->
    ValueRanges = ?ERL_MESOS_OBJ_TO_RECORD(value_ranges, ValueRangesObj),
    #value_ranges{range = ValueRangeObjs} = ValueRanges,
    ValueRanges#value_ranges{range =
        [?ERL_MESOS_OBJ_TO_RECORD(value_range, ValueRangeObj) ||
         ValueRangeObj <- ValueRangeObjs]}.

%% @doc Parses value set obj.
%% @private
-spec parse_value_set_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | value_set().
parse_value_set_obj(undefined) ->
    undefined;
parse_value_set_obj(ValueSetObj) ->
    ?ERL_MESOS_OBJ_TO_RECORD(value_set, ValueSetObj).
