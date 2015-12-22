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
framework_info_obj(#framework_info{id = Id,
                                   capabilities = Capabilities,
                                   labels = Labels} = FrameworkInfo) ->
    IdObj = framework_id_obj(Id),
    CapabilitiesObj = framework_info_capabilitie_obj(Capabilities),
    LabelsObj = labels_obj(Labels),
    FrameworkInfo1 =
        FrameworkInfo#framework_info{id = IdObj,
                                     capabilities = CapabilitiesObj,
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
                             operations = Operations} = CallAccept) ->
    OfferIdObjs = offer_id_objs(OfferIds),
    OperationObjs = offer_operation_objs(Operations),
    CallAccept1 = CallAccept#call_accept{offer_ids = OfferIdObjs,
                                         operations = OperationObjs},
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
offer_operation_obj(#offer_operation{launch = Launch} = OfferOperation) ->
    LaunchObj = offer_operation_launch_obj(Launch),
    OfferOperation1 = OfferOperation#offer_operation{launch = LaunchObj},
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
                         resources = Resources,
                         executor = Executor} = TaskInfo) ->
    TaskIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(task_id, TaskId),
    AgentIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(agent_id, AgentId),
    ResourceObjs = resource_objs(Resources),
    ExecutorObj = executor_info_obj(Executor),
    TaskInfo1 = TaskInfo#task_info{task_id = TaskIdObj,
                                   agent_id = AgentIdObj,
                                   resources = ResourceObjs,
                                   executor = ExecutorObj},
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
resource_obj(#resource{scalar = Scalar,
                       ranges = Ranges,
                       set = Set,
                       reservation = Reservation,
                       disk = Disk,
                       revocable = Revocable} = Resource) ->
    ScalarObj = value_scalar_obj(Scalar),
    RangesObj = value_ranges_obj(Ranges),
    SetObj = value_set_obj(Set),
    ReservationObj = resource_reservation_info_obj(Reservation),
    DiskObj = resource_disk_info_obj(Disk),
    RevocableObj = resource_revocable_info_obj(Revocable),
    Resource1 = Resource#resource{scalar = ScalarObj,
                                  ranges = RangesObj,
                                  set = SetObj,
                                  reservation = ReservationObj,
                                  disk = DiskObj,
                                  revocable = RevocableObj},
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
value_ranges_obj(#value_ranges{range = RangeList} = ValueRanges) ->
    RangeObjs = [?ERL_MESOS_OBJ_FROM_RECORD(value_range, Range) ||
                 Range <- RangeList],
    ValueRanges1 = ValueRanges#value_ranges{range = RangeObjs},
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
resource_disk_info_obj(#resource_disk_info{persistence = Persistence,
                                           volume = Volume} =
                       ResourceDiskInfo) ->
    PersistenceObj = resource_disk_info_persistence_obj(Persistence),
    VolumeObj = volume_obj(Volume),
    ResourceDiskInfo1 =
        ResourceDiskInfo#resource_disk_info{persistence = PersistenceObj,
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
image_obj(#image{appc = Appc,
                 docker = Docker} = Image) ->
    AppcObj = image_appc_obj(Appc),
    DockerObj = image_docker_obj(Docker),
    Image1 = Image#image{appc = AppcObj, docker = DockerObj},
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

%% @doc Returns executor info obj.
%% @private
-spec executor_info_obj(undefined | executor_info()) ->
    undefined | erl_mesos_obj:data_obj().
executor_info_obj(undefined) ->
    undefined;
executor_info_obj(#executor_info{executor_id = ExecutorId,
                                 framework_id = FrameworkId,
                                 command = Command,
                                 container = Container} = ExecutorInfo) ->
    ExecutorIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(executor_id, ExecutorId),
    FrameworkIdObj = framework_id_obj(FrameworkId),
    CommandObj = command_info_obj(Command),
    ContainerObj = container_info_obj(Container),
    ExecutorInfo1 = ExecutorInfo#executor_info{executor_id = ExecutorIdObj,
                                               framework_id = FrameworkIdObj,
                                               command = CommandObj,
                                               container = ContainerObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(executor_info, ExecutorInfo1).

%% @doc Returns command info obj.
%% @private
-spec command_info_obj(undefined | command_info()) ->
    undefined | erl_mesos_obj:data_obj().
command_info_obj(undefined) ->
    undefined;
command_info_obj(#command_info{container = Container,
                               uris = Uris,
                               environment = Environment} = CommandInfo) ->
    ContainerObj = command_info_container_info_obj(Container),
    UriObjs = command_info_uri_objs(Uris),
    EnvironmentObj = environment_obj(Environment),
    CommandInfo1 = CommandInfo#command_info{container = ContainerObj,
                                            uris = UriObjs,
                                            environment = EnvironmentObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(command_info, CommandInfo1).

%% @doc Returns command info container info obj.
%% @private
-spec command_info_container_info_obj(undefined |
                                      command_info_container_info()) ->
    undefined | erl_mesos_obj:data_obj().
command_info_container_info_obj(undefined) ->
    undefined;
command_info_container_info_obj(CommandInfoContainerInfo) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(command_info_container_info,
                               CommandInfoContainerInfo).

%% @doc Returns command info uri objs.
%% @private
-spec command_info_uri_objs(undefined | [command_info_uri()]) ->
    undefined | [erl_mesos_obj:data_obj()].
command_info_uri_objs(undefined) ->
    undefined;
command_info_uri_objs(CommandInfoUris) ->
    [?ERL_MESOS_OBJ_FROM_RECORD(command_info_uri, CommandInfoUri) ||
     CommandInfoUri <- CommandInfoUris].

%% @doc Returns environment obj.
%% @private
-spec environment_obj(undefined | environment()) ->
    undefined | erl_mesos_obj:data_obj().
environment_obj(undefined) ->
    undefined;
environment_obj(#environment{variables = Variables} = Environment) ->
    VariableObjs = [?ERL_MESOS_OBJ_FROM_RECORD(environment_variable, Variable)
                    || Variable <- Variables],
    Environment1 = Environment#environment{variables = VariableObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(environment, Environment1).

%% @doc Returns container info obj.
%% @private
-spec container_info_obj(undefined | container_info()) ->
    undefined | erl_mesos_obj:data_obj().
container_info_obj(undefined) ->
    undefined;
container_info_obj(#container_info{volumes = Volumes,
                                   docker = Docker,
                                   mesos = Mesos,
                                   network_infos = NetworkInfos} =
                   ContainerInfo) ->
    VolumeObjs = volume_objs(Volumes),
    DockerObj = container_info_docker_info_obj(Docker),
    MesosObj = container_info_mesos_info_obj(Mesos),
    NetworkInfoObjs = network_info_objs(NetworkInfos),
    ContainerInfo1 =
        ContainerInfo#container_info{volumes = VolumeObjs,
                                     docker = DockerObj,
                                     mesos = MesosObj,
                                     network_infos = NetworkInfoObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(container_info, ContainerInfo1).

%% @doc Returns volume objs.
%% @private
-spec volume_objs(undefined | [volume()]) ->
    undefined | [erl_mesos_obj:data_obj()].
volume_objs(undefined) ->
    undefined;
volume_objs(Volumes) ->
    [volume_obj(Volume) || Volume <- Volumes].

%% @doc Returns container info docker info obj.
%% @private
-spec container_info_docker_info_obj(undefined | container_info_docker_info()) ->
    undefined | erl_mesos_obj:data_obj().
container_info_docker_info_obj(undefined) ->
    undefined;
container_info_docker_info_obj(#container_info_docker_info{port_mappings =
                                                               PortMappings,
                                                           parameters =
                                                               Parameters} =
                               ContainerInfoDockerInfo) ->
    PortMappingObjs =
        container_info_docker_info_port_mapping_objs(PortMappings),
    ParameterObjs = parameter_objs(Parameters),
    ContainerInfoDockerInfo1 =
        ContainerInfoDockerInfo#container_info_docker_info{port_mappings =
                                                               PortMappingObjs,
                                                           parameters =
                                                               ParameterObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(container_info_docker_info,
                               ContainerInfoDockerInfo1).

%% @doc Returns container info docker info port mapping objs.
%% @private
-spec container_info_docker_info_port_mapping_objs(undefined |
          [container_info_docker_info_port_mapping()]) ->
    undefined | [erl_mesos_obj:data_obj()].
container_info_docker_info_port_mapping_objs(undefined) ->
    undefined;
container_info_docker_info_port_mapping_objs(PortMappings) ->
    [?ERL_MESOS_OBJ_FROM_RECORD(container_info_docker_info_port_mapping,
                                PortMapping) ||
     PortMapping <- PortMappings].

%% @doc Returns container info mesos info obj.
%% @private
-spec container_info_mesos_info_obj(undefined | container_info_mesos_info()) ->
    undefined | erl_mesos_obj:data_obj().
container_info_mesos_info_obj(undefined) ->
    undefined;
container_info_mesos_info_obj(#container_info_mesos_info{image = Image} =
                              ContainerInfoMesosInfo) ->
    ImageObj = image_obj(Image),
    ContainerInfoMesosInfo1 =
        ContainerInfoMesosInfo#container_info_mesos_info{image = ImageObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(container_info_mesos_info,
                               ContainerInfoMesosInfo1).

%% @doc Returns network info objs.
%% @private
-spec network_info_objs(undefined | [network_info()]) ->
    undefined | [erl_mesos_obj:data_obj()].
network_info_objs(undefined) ->
    undefined;
network_info_objs(NetworkInfos) ->
    [network_info_obj(NetworkInfo) || NetworkInfo <- NetworkInfos].

%% @doc Returns network info obj.
%% @private
-spec network_info_obj(network_info()) -> erl_mesos_obj:data_obj().
network_info_obj(#network_info{ip_addresses = IpAddresses,
                               labels = Labels} = NetworkInfo) ->
    IpAddressObjs = network_info_ip_address_objs(IpAddresses),
    LabelsObj = labels_obj(Labels),
    NetworkInfo1 = NetworkInfo#network_info{ip_addresses = IpAddressObjs,
                                            labels = LabelsObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(network_info, NetworkInfo1).

%% @doc Returns network info ip address objs.
%% @private
-spec network_info_ip_address_objs(undefined | [network_info_ip_address()]) ->
    undefined | [erl_mesos_obj:data_obj()].
network_info_ip_address_objs(undefined) ->
    undefined;
network_info_ip_address_objs(NetworkInfoIpAddresses) ->
    [?ERL_MESOS_OBJ_FROM_RECORD(network_info_ip_address, NetworkInfoIpAddress)
     || NetworkInfoIpAddress <- NetworkInfoIpAddresses].

%% @doc Returns parameter objs.
%% @private
-spec parameter_objs(undefined | [parameter()]) ->
    undefined | [erl_mesos_obj:data_obj()].
parameter_objs(undefined) ->
    undefined;
parameter_objs(Parameters) ->
    [?ERL_MESOS_OBJ_FROM_RECORD(parameter, Parameter) ||
     Parameter <- Parameters].

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
