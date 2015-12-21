-module(erl_mesos_scheduler_event).

-include("erl_mesos.hrl").

-include("erl_mesos_obj.hrl").

-export([parse_obj/1]).

-define(DEFAULT_HEARTBEAT_INTERVAL_SECONDS, 15.0).

%% External functions.

%% @doc Parses obj.
-spec parse_obj(erl_mesos_obj:data_obj()) -> event().
parse_obj(EventObj) ->
    Event = ?ERL_MESOS_OBJ_TO_RECORD(event, EventObj),
    parse_event(Event).

%% Internal functions.

%% @doc Parses event.
%% @private
-spec parse_event(event()) -> event().
parse_event(#event{type = <<"SUBSCRIBED">>,
                   subscribed = EventSubscribedObj} = Event) ->
    EventSubscribed = parse_event_subscribed_obj(EventSubscribedObj),
    Event#event{type = subscribed, subscribed = EventSubscribed};
parse_event(#event{type = <<"OFFERS">>,
                   offers = EventOffersObj} = Event) ->
    EventOffers = parse_event_offers_obj(EventOffersObj),
    Event#event{type = offers, offers = EventOffers};
parse_event(#event{type = <<"RESCIND">>,
                   rescind = EventRescindObj} = Event) ->
    EventRescind = ?ERL_MESOS_OBJ_TO_RECORD(event_rescind, EventRescindObj),
    OfferId = ?ERL_MESOS_OBJ_TO_RECORD(offer_id,
                                       EventRescind#event_rescind.offer_id),
    EventRescind1 = EventRescind#event_rescind{offer_id = OfferId},
    Event#event{type = rescind, rescind = EventRescind1};
parse_event(#event{type = <<"ERROR">>, error = EventErrorObj} = Event) ->
    EventError = ?ERL_MESOS_OBJ_TO_RECORD(event_error, EventErrorObj),
    Event#event{type = error, error = EventError};
parse_event(#event{type = <<"HEARTBEAT">>} = Event) ->
    Event#event{type = heartbeat}.

%% @doc Parses event subscribe obj.
%% @private
-spec parse_event_subscribed_obj(erl_mesos_obj:data_obj()) ->
    event_subscribed().
parse_event_subscribed_obj(EventSubscribedObj) ->
    EventSubscribed =
        ?ERL_MESOS_OBJ_TO_RECORD(event_subscribed, EventSubscribedObj),
    FrameworkId =
        ?ERL_MESOS_OBJ_TO_RECORD(framework_id,
                                 EventSubscribed#event_subscribed.framework_id),
    HeartbeatIntervalSeconds =
        parse_heartbeat_interval_seconds(
            EventSubscribed#event_subscribed.heartbeat_interval_seconds),
    EventSubscribed#event_subscribed{framework_id = FrameworkId,
                                     heartbeat_interval_seconds =
                                         HeartbeatIntervalSeconds}.

%% @doc Returns heartbeat interval.
%% @private
-spec parse_heartbeat_interval_seconds(undefined | float()) -> float().
parse_heartbeat_interval_seconds(undefined) ->
    ?DEFAULT_HEARTBEAT_INTERVAL_SECONDS;
parse_heartbeat_interval_seconds(HeartbeatIntervalSeconds) ->
    HeartbeatIntervalSeconds.

%% @doc Parses event offers obj.
%% @private
-spec parse_event_offers_obj(erl_mesos_obj:data_obj()) -> event_offers().
parse_event_offers_obj(EventOffersObj) ->
    EventOffers = ?ERL_MESOS_OBJ_TO_RECORD(event_offers, EventOffersObj),
    Offers = parse_offer_objs(EventOffers#event_offers.offers),
    InverseOffers =
        parse_inverse_offer_objs(EventOffers#event_offers.inverse_offers),
    EventOffers#event_offers{offers = Offers,
                             inverse_offers = InverseOffers}.

%% @doc Parses offer objs.
%% @private
-spec parse_offer_objs(undefined | [erl_mesos_obj:data_obj()]) ->
    undefined | [offer()].
parse_offer_objs(undefined) ->
    undefined;
parse_offer_objs(OfferObjs) ->
    [parse_offer_obj(OfferObj) || OfferObj <- OfferObjs].

%% @doc Parses offer obj.
%% @private
-spec parse_offer_obj(erl_mesos_obj:data_obj()) -> offer().
parse_offer_obj(OfferObj) ->
    Offer = ?ERL_MESOS_OBJ_TO_RECORD(offer, OfferObj),
    Id = ?ERL_MESOS_OBJ_TO_RECORD(offer_id, Offer#offer.id),
    FrameworkId = ?ERL_MESOS_OBJ_TO_RECORD(framework_id,
                                           Offer#offer.framework_id),
    AgentId = ?ERL_MESOS_OBJ_TO_RECORD(agent_id, Offer#offer.agent_id),
    Url = parse_url_obj(Offer#offer.url),
    Resources = parse_resource_objs(Offer#offer.resources),
    Attributes = parse_attribute_objs(Offer#offer.attributes),
    ExecutorIds = parse_executor_id_objs(Offer#offer.executor_ids),
    Unavailability = parse_unavailability_obj(Offer#offer.unavailability),
    Offer#offer{id = Id,
                framework_id = FrameworkId,
                agent_id = AgentId,
                url = Url,
                resources = Resources,
                attributes = Attributes,
                executor_ids = ExecutorIds,
                unavailability = Unavailability}.

%% @doc Parses inverse offer objs.
%% @private
-spec parse_inverse_offer_objs(undefined | erl_mesos_obj:data_obj()) ->
    undefined | [inverse_offer()].
parse_inverse_offer_objs(undefined) ->
    undefined;
parse_inverse_offer_objs(InverseOfferObjs) ->
    [parse_inverse_offer_obj(InverseOfferObj) ||
     InverseOfferObj <- InverseOfferObjs].

%% @doc Parses inverse offer obj.
%% @private
-spec parse_inverse_offer_obj(erl_mesos_obj:data_obj()) -> inverse_offer().
parse_inverse_offer_obj(InverseOfferObj) ->
    InverseOffer = ?ERL_MESOS_OBJ_TO_RECORD(inverse_offer, InverseOfferObj),
    Id = ?ERL_MESOS_OBJ_TO_RECORD(offer_id, InverseOffer#inverse_offer.id),
    Url = parse_url_obj(InverseOffer#inverse_offer.url),
    FrameworkId =
        ?ERL_MESOS_OBJ_TO_RECORD(framework_id,
                                 InverseOffer#inverse_offer.framework_id),
    AgentId = ?ERL_MESOS_OBJ_TO_RECORD(agent_id,
                                       InverseOffer#inverse_offer.agent_id),
    Unavailability =
        parse_unavailability_obj(InverseOffer#inverse_offer.unavailability),
    Resources = parse_resource_objs(InverseOffer#inverse_offer.resources),
    InverseOffer#inverse_offer{id = Id,
                               url = Url,
                               framework_id = FrameworkId,
                               agent_id = AgentId,
                               unavailability = Unavailability,
                               resources = Resources}.

%% @doc Parses url obj.
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
    [parse_resource_obj(ResourceObj) || ResourceObj <- ResourceObjs].

%% @doc Parses resource obj.
%% @private
-spec parse_resource_obj(erl_mesos_obj:data_obj()) -> resource().
parse_resource_obj(ResourceObj) ->
    Resource = ?ERL_MESOS_OBJ_TO_RECORD(resource, ResourceObj),
    Scalar = parse_value_scalar_obj(Resource#resource.scalar),
    Ranges = parse_value_ranges_obj(Resource#resource.ranges),
    Set = parse_value_set_obj(Resource#resource.set),
    Reservation =
        parse_resource_reservation_info_obj(Resource#resource.reservation),
    Disk = parse_resource_disk_info_obj(Resource#resource.disk),
    Revocable = parse_resource_revocable_info_obj(Resource#resource.revocable),
    Resource#resource{scalar = Scalar,
                      ranges = Ranges,
                      set = Set,
                      reservation = Reservation,
                      disk = Disk,
                      revocable = Revocable}.

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

%% @doc Parses value text obj.
%% @private
-spec parse_value_text_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | value_text().
parse_value_text_obj(undefined) ->
    undefined;
parse_value_text_obj(ValueTextObj) ->
    ?ERL_MESOS_OBJ_TO_RECORD(value_text, ValueTextObj).

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

%% @doc Parses image docker obj.
%% @private
-spec parse_image_docker_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | image_docker().
parse_image_docker_obj(undefined) ->
    undefined;
parse_image_docker_obj(ImageDockerObj) ->
    ?ERL_MESOS_OBJ_TO_RECORD(image_docker, ImageDockerObj).

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

%% @doc Parses resource revocable info obj.
%% @private
-spec parse_resource_revocable_info_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | resource_revocable_info().
parse_resource_revocable_info_obj(undefined) ->
    undefined;
parse_resource_revocable_info_obj(ResourceRevocableInfoObj) ->
    ?ERL_MESOS_OBJ_TO_RECORD(resource_revocable_info,
                             ResourceRevocableInfoObj).

%% @doc Parses attribute objs.
%% @private
-spec parse_attribute_objs(undefined | [erl_mesos_obj:data_obj()]) ->
    undefined | [attribute()].
parse_attribute_objs(undefined) ->
    undefined;
parse_attribute_objs(AttributeObjs) ->
    [parse_attribute_obj(AttributeObj) || AttributeObj <- AttributeObjs].

%% @doc Parses attribute obj.
%% @private
-spec parse_attribute_obj(erl_mesos_obj:data_obj()) -> attribute().
parse_attribute_obj(AttributeObj) ->
    Attribute = ?ERL_MESOS_OBJ_TO_RECORD(attribute, AttributeObj),
    Scalar = parse_value_scalar_obj(Attribute#attribute.scalar),
    Ranges = parse_value_ranges_obj(Attribute#attribute.ranges),
    Set = parse_value_set_obj(Attribute#attribute.set),
    Text = parse_value_text_obj(Attribute#attribute.text),
    Attribute#attribute{scalar = Scalar,
                        ranges = Ranges,
                        set = Set,
                        text = Text}.

%% @doc Parses executor id objs.
%% @private
-spec parse_executor_id_objs(undefined | [erl_mesos_obj:data_obj()]) ->
    undefined | [executor_id()].
parse_executor_id_objs(undefined) ->
    undefined;
parse_executor_id_objs(ExecutorIdObjs) ->
    [?ERL_MESOS_OBJ_TO_RECORD(executor_id, ExecutorIdObj) ||
     ExecutorIdObj <- ExecutorIdObjs].

%% @doc Parses unavailability obj.
%% @private
-spec parse_unavailability_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | unavailability().
parse_unavailability_obj(undefined) ->
    undefined;
parse_unavailability_obj(UnavailabilityObj) ->
    Unavailability = ?ERL_MESOS_OBJ_TO_RECORD(unavailability,
                                              UnavailabilityObj),
    Start = parse_time_info_obj(Unavailability#unavailability.start),
    Duration = parse_duration_info_obj(Unavailability#unavailability.duration),
    Unavailability#unavailability{start = Start, duration = Duration}.

%% @doc Parses time info obj.
%% @private
-spec parse_time_info_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | time_info().
parse_time_info_obj(undefined) ->
    undefined;
parse_time_info_obj(TimeInfoObj) ->
    ?ERL_MESOS_OBJ_TO_RECORD(time_info, TimeInfoObj).

%% @doc Parses duration info obj.
%% @private
-spec parse_duration_info_obj(undefined | erl_mesos_obj:data_obj()) ->
    undefined | duration_info().
parse_duration_info_obj(undefined) ->
    undefined;
parse_duration_info_obj(DurationInfoObj) ->
    ?ERL_MESOS_OBJ_TO_RECORD(duration_info, DurationInfoObj).
