-module(erl_mesos_scheduler_call).

-include("erl_mesos.hrl").

-include("erl_mesos_obj.hrl").

-export([subscribe/2, accept/2]).

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

%% Executes accept call.
-spec accept(scheduler_info(), call_subscribe()) -> ok | {error, term()}.
accept(#scheduler_info{subscribed = false}, _CallAccept) ->
    {error, not_subscribed};
accept(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
       CallAccept) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    CallAcceptObj = call_accept_obj(CallAccept),
    Call = #call{type = <<"ACCEPT">>, accept = CallAcceptObj},
    CallObj = call_obj(SchedulerInfo, Call),
    handle_response(request(SchedulerInfo1, CallObj)).

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

%% @doc Returns request options.
%% @private
-spec request_options(erl_mesos_http:options()) -> erl_mesos_http:options().
request_options(RequestOptions) ->
    proplists:delete(async, lists:delete(async, RequestOptions)).

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

%% @doc Returns framework info obj.
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

%% @doc Returns framework info capabilitie obj.
%% @private
-spec framework_info_capabilitie_obj(undefined | erl_mesos_obj:data_string()) ->
    undefined | erl_mesos_obj:data_obj().
framework_info_capabilitie_obj(undefined) ->
    undefined;
framework_info_capabilitie_obj(FrameworkInfoCapabilitie) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(framework_info_capabilitie,
                               FrameworkInfoCapabilitie).

%% @doc Returns labels obj.
%% @private
-spec labels_obj(undefined | labels()) -> erl_mesos_obj:data_obj().
labels_obj(undefined) ->
    undefined;
labels_obj(#labels{labels = LabelsList} = Labels) ->
    LabelObjs = [?ERL_MESOS_OBJ_FROM_RECORD(label, Label) ||
                 Label <- LabelsList],
    Labels1 = Labels#labels{labels = LabelObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(labels, Labels1).

%% @doc Returns call accept obj.
%% @private
-spec call_accept_obj(call_accept()) -> erl_mesos_obj:data_obj().
call_accept_obj(#call_accept{offer_ids = OfferIds,
                             operations = OfferOperations} = CallAccept) ->
    OfferIdObjs = offer_id_objs(OfferIds),
    OfferOperationObjs = offer_operation_objs(OfferOperations),
    CallAccept1 = CallAccept#call_accept{offer_ids = OfferIdObjs,
                                         operations = OfferOperationObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(call_accept, CallAccept1).

%% @doc Returns offer id objs.
%% @private
-spec offer_id_objs(undefined | [offer_id()]) ->
    undefined | [erl_mesos_obj:data_obj()].
offer_id_objs(undefined) ->
    undefined;
offer_id_objs(OfferIds) ->
    [?ERL_MESOS_OBJ_FROM_RECORD(offer_id, OfferId) || OfferId <- OfferIds].

%% @doc Returns offer operation objs.
%% @private
-spec offer_operation_objs(undefined | [offer_operation()]) ->
    undefined | [erl_mesos_obj:data_obj()].
offer_operation_objs(undefined) ->
    undefined;
offer_operation_objs(OfferOperations) ->
    [offer_operation_obj(OfferOperation) || OfferOperation <- OfferOperations].

%% @doc Returns offer operation obj.
%% @private
-spec offer_operation_obj(offer_operation()) -> erl_mesos_obj:data_obj().
offer_operation_obj(#offer_operation{launch = OfferOperationLaunch} =
                    OfferOperation) ->
    OfferOperationLaunchObj = offer_operation_launch_obj(OfferOperationLaunch),
    OfferOperation1 =
        OfferOperation#offer_operation{launch = OfferOperationLaunchObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(offer_operation, OfferOperation1).

%% @doc Returns offer operation launch obj.
%% @private
-spec offer_operation_launch_obj(undefined | offer_operation_launch()) ->
    undefined | erl_mesos_obj:data_obj().
offer_operation_launch_obj(undefined) ->
    undefined;
offer_operation_launch_obj(#offer_operation_launch{task_infos = TaskInfos} =
                           OfferOperationLaunch) ->
    TaskInfoObjs = [task_info_obj(TaskInfo) || TaskInfo <- TaskInfos],
    OfferOperationLaunch1 =
        OfferOperationLaunch#offer_operation_launch{task_infos = TaskInfoObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(offer_operation_launch, OfferOperationLaunch1).

%% @doc Returns task info obj.
%% @private
-spec task_info_obj(task_info()) -> erl_mesos_obj:data_obj().
task_info_obj(#task_info{task_id = TaskId,
                         agent_id = AgentId,
                         resources = Resources} = TaskInfo) ->
    TaskIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(task_id, TaskId),
    AgentIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(agent_id, AgentId),
    ResourceObjs = resource_objs(Resources),
    TaskInfo1 = TaskInfo#task_info{task_id = TaskIdObj,
                                   agent_id = AgentIdObj,
                                   resources = ResourceObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(task_info, TaskInfo1).

%% @doc Returns resource objs.
%% @private
-spec resource_objs(undefined | [resource()]) ->
    undefined | [erl_mesos_obj:data_obj()].
resource_objs(undefined) ->
    undefined;
resource_objs(Resources) ->
    [resource_obj(Resource) || Resource <- Resources].

%% @doc Returns resource obj.
%% @private
-spec resource_obj(resource()) -> erl_mesos_obj:data_obj().
resource_obj(#resource{scalar = ValueScalar,
                       ranges = ValueRanges,
                       set = ValueSet,
                       reservation = ResourceReservationInfo,
                       disk = ResourceDiskInfo,
                       revocable = ResourceRevocableInfo} = Resource) ->
    ValueScalarObj = value_scalar_obj(ValueScalar),
    ValueRangesObj = value_ranges_obj(ValueRanges),
    ValueSetObj = value_set_obj(ValueSet),
    ResourceReservationInfoObj =
        resource_reservation_info_obj(ResourceReservationInfo),
    ResourceDiskInfoObj = resource_disk_info_obj(ResourceDiskInfo),
    ResourceRevocableInfoObj =
        resource_revocable_info_obj(ResourceRevocableInfo),
    Resource1 = Resource#resource{scalar = ValueScalarObj,
                                  ranges = ValueRangesObj,
                                  set = ValueSetObj,
                                  reservation = ResourceReservationInfoObj,
                                  disk = ResourceDiskInfoObj,
                                  revocable = ResourceRevocableInfoObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(resource, Resource1).

%% @doc Returns value scalar obj.
%% @private
-spec value_scalar_obj(undefined | value_scalar()) ->
    undefined | erl_mesos_obj:data_obj().
value_scalar_obj(undefined) ->
    undefined;
value_scalar_obj(ValueScalar) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(value_scalar, ValueScalar).

%% @doc Returns value ranges obj.
%% @private
-spec value_ranges_obj(undefined | value_ranges()) ->
    undefined | erl_mesos_obj:data_obj().
value_ranges_obj(undefined) ->
    undefined;
value_ranges_obj(#value_ranges{range = ValueRangesList} = ValueRanges) ->
    ValueRangeObjs = [?ERL_MESOS_OBJ_FROM_RECORD(value_range, ValueRange) ||
                      ValueRange <- ValueRangesList],
    ValueRanges1 = ValueRanges#value_ranges{range = ValueRangeObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(value_ranges, ValueRanges1).

%% @doc Returns value set obj.
%% @private
-spec value_set_obj(undefined | value_set()) ->
    undefined | erl_mesos_obj:data_obj().
value_set_obj(undefined) ->
    undefined;
value_set_obj(ValueSet) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(value_set, ValueSet).

%% @doc Returns resource reservation info obj.
%% @private
-spec resource_reservation_info_obj(undefined | resource_reservation_info()) ->
    undefined | erl_mesos_obj:data_obj().
resource_reservation_info_obj(undefined) ->
    undefined;
resource_reservation_info_obj(ResourceReservationInfo) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(resource_reservation_info,
                               ResourceReservationInfo).

%% @doc Returns resource disk info obj.
%% @private
-spec resource_disk_info_obj(undefined | resource_disk_info()) ->
    undefined | erl_mesos_obj:data_obj().
resource_disk_info_obj(undefined) ->
    undefined;
resource_disk_info_obj(#resource_disk_info{persistence =
                                               ResourceDiskInfoPersistence,
                                           volume = Volume} =
                       ResourceDiskInfo) ->
    ResourceDiskInfoPersistenceObj =
        resource_disk_info_persistence_obj(ResourceDiskInfoPersistence),
    VolumeObj = volume_obj(Volume),
    ResourceDiskInfo1 =
        ResourceDiskInfo#resource_disk_info{persistence =
                                                ResourceDiskInfoPersistenceObj,
                                            volume = VolumeObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(resource_disk_info, ResourceDiskInfo1).

%% @doc Returns resource disk info persistence obj.
%% @private
-spec resource_disk_info_persistence_obj(undefined |
                                         resource_disk_info_persistence()) ->
    undefined | erl_mesos_obj:data_obj().
resource_disk_info_persistence_obj(undefined) ->
    undefined;
resource_disk_info_persistence_obj(ResourceDiskInfoPersistence) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(resource_disk_info_persistence,
                               ResourceDiskInfoPersistence).

%% @doc Returns volume obj.
%% @private
-spec volume_obj(undefined | volume()) -> undefined | erl_mesos_obj:data_obj().
volume_obj(undefined) ->
    undefined;
volume_obj(#volume{image = Image} = Volume) ->
    ImageObj = image_obj(Image),
    Volume1 = Volume#volume{image = ImageObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(volume, Volume1).

%% @doc Returns image obj.
%% @private
-spec image_obj(undefined | image()) -> undefined | erl_mesos_obj:data_obj().
image_obj(undefined) ->
    undefined;
image_obj(#image{appc = ImageAppc,
                 docker = ImageDocker} = Image) ->
    ImageAppcObj = image_appc_obj(ImageAppc),
    ImageDockerObj = image_docker_obj(ImageDocker),
    Image1 = Image#image{appc = ImageAppcObj,
                         docker = ImageDockerObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(image, Image1).

%% @doc Returns image appc obj.
%% @private
-spec image_appc_obj(undefined | image_appc()) ->
    undefined | erl_mesos_obj:data_obj().
image_appc_obj(undefined) ->
    undefined;
image_appc_obj(#image_appc{labels = Labels} = ImageAppc) ->
    LabelsObj = labels_obj(Labels),
    ImageAppc1 = ImageAppc#image_appc{labels = LabelsObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(image_appc, ImageAppc1).

%% @doc Returns image docker obj.
%% @private
-spec image_docker_obj(undefined | image_docker()) ->
    undefined | erl_mesos_obj:data_obj().
image_docker_obj(undefined) ->
    undefined;
image_docker_obj(ImageDocker) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(image_docker, ImageDocker).

%% @doc Returns resource revocable info obj.
%% @private
-spec resource_revocable_info_obj(undefined | resource_revocable_info()) ->
    undefined | erl_mesos_obj:data_obj().
resource_revocable_info_obj(undefined) ->
    undefined;
resource_revocable_info_obj(ResourceRevocableInfo) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(resource_revocable_info, ResourceRevocableInfo).

%% %% @doc Returns discovery info obj.
%% %% @private
%% -spec discovery_info_obj(undefined | discovery_info()) ->
%%     undefined | erl_mesos_obj:data_obj().
%% discovery_info_obj(undefined) ->
%%     undefined;
%% discovery_info_obj(#discovery_info{ports = Ports,
%%                                    labels = Labels} = DiscoveryInfo) ->
%%     PortsObj = ports_obj(Ports),
%%     LabelsObj = labels_obj(Labels),
%%     DiscoveryInfo1 = DiscoveryInfo#discovery_info{ports = PortsObj,
%%                                                   labels = LabelsObj},
%%     ?ERL_MESOS_OBJ_FROM_RECORD(discovery_info, DiscoveryInfo1).
%%
%% %% @doc Returns ports obj.
%% %% @private
%% -spec ports_obj(undefined | pts()) -> undefined | erl_mesos_obj:data_obj().
%% ports_obj(undefined) ->
%%     undefined;
%% ports_obj(#ports{ports = PortsList} = Ports) ->
%%     PortObjs = [?ERL_MESOS_OBJ_FROM_RECORD(port, Port) || Port <- PortsList],
%%     Ports1 = Ports#ports{ports = PortObjs},
%%     ?ERL_MESOS_OBJ_FROM_RECORD(ports, Ports1).

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

%% @doc Handles response.
%% @private
-spec handle_response({ok, non_neg_integer(), erl_mesos_http:headers(),
                      reference()} | {error, term()}) ->
    ok | {error, term()} |
    {error, {http_response, non_neg_integer(), binary()}}.
handle_response(Response) ->
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
