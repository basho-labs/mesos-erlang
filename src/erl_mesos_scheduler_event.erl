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
    Offers = parse_offer_objs(OfferObjs, []),
    OffersEvent = #offers_event{offers = Offers},
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
    Url = parse_url_obj(Offer#offer.url),
    Resources = parse_resource_objs(Offer#offer.resources),
    Offer1 = Offer#offer{id = Id,
                         framework_id = FrameworkId,
                         agent_id = AgentId,
                         url = Url,
                         resources = Resources},
    parse_offer_objs(OfferObjs, [Offer1 | Offers]);
parse_offer_objs([], Offers) ->
    lists:reverse(Offers).

%% @doc Parses url objs.
%% @private
-spec parse_url_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | url().
parse_url_obj(UrlObj) ->
    Url = ?ERL_MESOS_OBJ_TO_RECORD(url, UrlObj),
    Address = ?ERL_MESOS_OBJ_TO_RECORD(address, Url#url.address),
    Query = parse_parameter_objs(Url#url.query),
    Url#url{address = Address,
            query = Query}.

%% @doc Parses parameter objs.
%% @private
-spec parse_parameter_objs(undefined | erl_mesos_obj:data_obj()) ->
    undefined | [parameter()].
parse_parameter_objs(undefined) ->
    undefined;
parse_parameter_objs(ParameterObjs) ->
    [?ERL_MESOS_OBJ_TO_RECORD(parameter, ParameterObj) ||
     ParameterObj <- ParameterObjs].

%% @doc Parses resource objs.
%% @private
-spec parse_resource_objs(undefined | [erl_mesos_obj:data_obj()]) ->
    undefined | [resource()].
parse_resource_objs(undefined) ->
    undefined;
parse_resource_objs(ResourceObjs) ->
    parse_resource_objs(ResourceObjs, []).

%% @doc Parses resource objs.
%% @private
-spec parse_resource_objs([erl_mesos_obj:data_obj()], [resource()]) ->
    undefined | [resource()].
parse_resource_objs([ResourceObj | ResourceObjs], Resources) ->
    Resource = ?ERL_MESOS_OBJ_TO_RECORD(resource, ResourceObj),
    Scalar = parse_value_scalar_obj(Resource#resource.scalar),
    Ranges = parse_value_ranges_obj(Resource#resource.ranges),
    Set = parse_value_set_obj(Resource#resource.set),
    Reservation =
        parse_resource_reservation_info_obj(Resource#resource.reservation),
    Disk = parse_resource_disk_info_obj(Resource#resource.disk),
    Revocable = parse_resource_revocable_info_obj(Resource#resource.revocable),
    Resource1 = Resource#resource{scalar = Scalar,
                                  ranges = Ranges,
                                  set = Set,
                                  reservation = Reservation,
                                  disk = Disk,
                                  revocable = Revocable},
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
    ValueRanges#value_ranges{range =
        [?ERL_MESOS_OBJ_TO_RECORD(value_range, ValueRangeObj) ||
         ValueRangeObj <- ValueRanges#value_ranges.range]}.

%% @doc Parses value set obj.
%% @private
-spec parse_value_set_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | value_set().
parse_value_set_obj(undefined) ->
    undefined;
parse_value_set_obj(ValueSetObj) ->
    ?ERL_MESOS_OBJ_TO_RECORD(value_set, ValueSetObj).

%% @doc Parses resource reservation info obj.
%% @private
-spec parse_resource_reservation_info_obj(undefined |
                                          erl_mesos_obj:data_obj()) ->
    undefined | resource_reservation_info().
parse_resource_reservation_info_obj(undefined) ->
    undefined;
parse_resource_reservation_info_obj(ResourceReservationInfoObj) ->
    ?ERL_MESOS_OBJ_TO_RECORD(resource_reservation_info,
                             ResourceReservationInfoObj).

%% @doc Parses resource reservation info obj.
%% @private
-spec parse_resource_disk_info_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | resource_disk_info().
parse_resource_disk_info_obj(undefined) ->
    undefined;
parse_resource_disk_info_obj(ResourceDiskInfoObj) ->
    ResourceDiskInfo = ?ERL_MESOS_OBJ_TO_RECORD(resource_disk_info,
                                                ResourceDiskInfoObj),
    PersistenceObj = ResourceDiskInfo#resource_disk_info.persistence,
    Persistence = parse_resource_disk_info_persistence_obj(PersistenceObj),
    Volume = parse_volume_obj(ResourceDiskInfo#resource_disk_info.volume),
    ResourceDiskInfo#resource_disk_info{persistence = Persistence,
                                        volume = Volume}.

%% @doc Parses resource disk info persistence obj.
%% @private
-spec parse_resource_disk_info_persistence_obj(undefined |
                                               erl_mesos_obj:data_obj()) ->
    undefined | resource_disk_info_persistence().
parse_resource_disk_info_persistence_obj(undefined) ->
    undefined;
parse_resource_disk_info_persistence_obj(ResourceDiskInfoPersistenceObj) ->
    ?ERL_MESOS_OBJ_TO_RECORD(resource_disk_info_persistence,
                             ResourceDiskInfoPersistenceObj).

%% @doc Parses volume obj.
%% @private
-spec parse_volume_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | volume().
parse_volume_obj(undefined) ->
    undefined;
parse_volume_obj(VolumeObj) ->
    Volume = ?ERL_MESOS_OBJ_TO_RECORD(volume, VolumeObj),
    Image = parse_image_obj(Volume#volume.image),
    Volume#volume{image = Image}.

%% @doc Parses image obj.
%% @private
-spec parse_image_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | image().
parse_image_obj(undefined) ->
    undefined;
parse_image_obj(ImageObj) ->
    Image = ?ERL_MESOS_OBJ_TO_RECORD(image, ImageObj),
    Appc = parse_image_appc_obj(Image#image.appc),
    Docker = parse_image_docker_obj(Image#image.docker),
    Image#image{appc = Appc, docker = Docker}.

%% @doc Parses image appc obj.
%% @private
-spec parse_image_appc_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | image_appc().
parse_image_appc_obj(undefined) ->
    undefined;
parse_image_appc_obj(ImageAppcObj) ->
    ImageAppc = ?ERL_MESOS_OBJ_TO_RECORD(image_appc, ImageAppcObj),
    Labels = parse_labels_obj(ImageAppc#image_appc.labels),
    ImageAppc#image_appc{labels = Labels}.

%% @doc Parses labels obj.
%% @private
-spec parse_labels_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | labels().
parse_labels_obj(undefined) ->
    undefined;
parse_labels_obj(LabelsObj) ->
    Labels = ?ERL_MESOS_OBJ_TO_RECORD(labels, LabelsObj),
    Labels#labels{labels =
        [?ERL_MESOS_OBJ_TO_RECORD(label, LabelObj) ||
         LabelObj <- Labels#labels.labels]}.

%% @doc Parses image docker obj.
%% @private
-spec parse_image_docker_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | image_docker().
parse_image_docker_obj(undefined) ->
    undefined;
parse_image_docker_obj(ImageDockerObj) ->
    ?ERL_MESOS_OBJ_TO_RECORD(image_docker, ImageDockerObj).

%% @doc Parses resource revocable info obj.
%% @private
-spec parse_resource_revocable_info_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | resource_revocable_info().
parse_resource_revocable_info_obj(undefined) ->
    undefined;
parse_resource_revocable_info_obj(ResourceRevocableInfoObj) ->
    ?ERL_MESOS_OBJ_TO_RECORD(resource_revocable_info,
                             ResourceRevocableInfoObj).
