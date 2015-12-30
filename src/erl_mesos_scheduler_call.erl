%% @private

-module(erl_mesos_scheduler_call).

-include("erl_mesos.hrl").

-include("erl_mesos_obj.hrl").

-export([subscribe/2,
         teardown/1,
         accept/2,
         decline/2,
         revive/1,
         kill/2,
         shutdown/2,
         acknowledge/2,
         reconcile/2,
         message/2,
         request/2]).

-type version() :: v1.
-export_type([version/0]).

-define(V1_API_PATH, "/api/v1/scheduler").

%% External functions.

%% Executes subscribe call.
-spec subscribe(erl_mesos:scheduler_info(), erl_mesos:call_subscribe()) ->
    {ok, erl_mesos_http:client_ref()} | {error, term()}.
subscribe(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
          CallSubscribe) ->
    RequestOptions1 = subscribe_request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    CallSubscribeObj = call_subscribe_obj(CallSubscribe),
    Call = #call{type = <<"SUBSCRIBE">>, subscribe = CallSubscribeObj},
    CallObj = call_obj(SchedulerInfo, Call),
    send_request(SchedulerInfo1, CallObj).

%% Executes teardown call.
-spec teardown(erl_mesos:scheduler_info()) -> ok | {error, term()}.
teardown(#scheduler_info{subscribed = false}) ->
    {error, not_subscribed};
teardown(#scheduler_info{request_options = RequestOptions} = SchedulerInfo) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #call{type = <<"TEARDOWN">>},
    CallObj = call_obj(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, CallObj)).

%% Executes accept call.
-spec accept(erl_mesos:scheduler_info(), erl_mesos:call_accept()) ->
    ok | {error, term()}.
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
    handle_response(send_request(SchedulerInfo1, CallObj)).

%% Executes decline call.
-spec decline(erl_mesos:scheduler_info(), erl_mesos:call_decline()) ->
    ok | {error, term()}.
decline(#scheduler_info{subscribed = false}, _CallDecline) ->
    {error, not_subscribed};
decline(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
        CallDecline) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    CallDeclineObj = call_decline_obj(CallDecline),
    Call = #call{type = <<"DECLINE">>, decline = CallDeclineObj},
    CallObj = call_obj(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, CallObj)).

%% Executes revive call.
-spec revive(erl_mesos:scheduler_info()) -> ok | {error, term()}.
revive(#scheduler_info{subscribed = false}) ->
    {error, not_subscribed};
revive(#scheduler_info{request_options = RequestOptions} = SchedulerInfo) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    Call = #call{type = <<"REVIVE">>},
    CallObj = call_obj(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, CallObj)).

%% Executes kill call.
-spec kill(erl_mesos:scheduler_info(), erl_mesos:call_kill()) ->
    ok | {error, term()}.
kill(#scheduler_info{subscribed = false}, _CallKill) ->
    {error, not_subscribed};
kill(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
     CallKill) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    CallKillObj = call_kill_obj(CallKill),
    Call = #call{type = <<"KILL">>, kill = CallKillObj},
    CallObj = call_obj(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, CallObj)).

%% Executes shutdown call.
-spec shutdown(erl_mesos:scheduler_info(), erl_mesos:call_shutdown()) ->
    ok | {error, term()}.
shutdown(#scheduler_info{subscribed = false}, _CallShutdown) ->
    {error, not_subscribed};
shutdown(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
         CallShutdown) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    CallShutdownObj = call_shutdown_obj(CallShutdown),
    Call = #call{type = <<"SHUTDOWN">>, shutdown = CallShutdownObj},
    CallObj = call_obj(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, CallObj)).

%% Executes acknowledge call.
-spec acknowledge(erl_mesos:scheduler_info(), erl_mesos:call_acknowledge()) ->
    ok | {error, term()}.
acknowledge(#scheduler_info{subscribed = false}, _CallAcknowledge) ->
    {error, not_subscribed};
acknowledge(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
            CallAcknowledge) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    CallAcknowledgeObj = call_acknowledge_obj(CallAcknowledge),
    Call = #call{type = <<"ACKNOWLEDGE">>, acknowledge = CallAcknowledgeObj},
    CallObj = call_obj(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, CallObj)).

%% Executes reconcile call.
-spec reconcile(erl_mesos:scheduler_info(), erl_mesos:call_reconcile()) ->
    ok | {error, term()}.
reconcile(#scheduler_info{subscribed = false}, _CallReconcile) ->
    {error, not_subscribed};
reconcile(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
          CallReconcile) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    CallReconcileObj = call_reconcile_obj(CallReconcile),
    Call = #call{type = <<"RECONCILE">>, reconcile = CallReconcileObj},
    CallObj = call_obj(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, CallObj)).

%% Executes message call.
-spec message(erl_mesos:scheduler_info(), erl_mesos:call_message()) ->
    ok | {error, term()}.
message(#scheduler_info{subscribed = false}, _CallMessage) ->
    {error, not_subscribed};
message(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
        CallMessage) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    CallMessageObj = call_message_obj(CallMessage),
    Call = #call{type = <<"MESSAGE">>, message = CallMessageObj},
    CallObj = call_obj(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, CallObj)).

%% Executes request call.
-spec request(erl_mesos:scheduler_info(), erl_mesos:call_request()) ->
    ok | {error, term()}.
request(#scheduler_info{subscribed = false}, _CallRequest) ->
    {error, not_subscribed};
request(#scheduler_info{request_options = RequestOptions} = SchedulerInfo,
        CallRequest) ->
    RequestOptions1 = request_options(RequestOptions),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{request_options =
                                                  RequestOptions1},
    CallRequestObj = call_request_obj(CallRequest),
    Call = #call{type = <<"REQUEST">>, request = CallRequestObj},
    CallObj = call_obj(SchedulerInfo, Call),
    handle_response(send_request(SchedulerInfo1, CallObj)).

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
-spec call_obj(erl_mesos:scheduler_info(), erl_mesos:call()) ->
    erl_mesos_obj:data_obj().
call_obj(#scheduler_info{framework_id = FrameworkId}, Call) ->
    FrameworkIdObj = framework_id_obj(FrameworkId),
    Call1 = Call#call{framework_id = FrameworkIdObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(call, Call1).

%% @doc Returns call subscribe obj.
%% @private
-spec call_subscribe_obj(erl_mesos:call_subscribe()) ->
    erl_mesos_obj:data_obj().
call_subscribe_obj(#call_subscribe{framework_info = FrameworkInfo} =
                   CallSubscribe) ->
    FrameworkInfoObj = framework_info_obj(FrameworkInfo),
    CallSubscribe1 =
        CallSubscribe#call_subscribe{framework_info = FrameworkInfoObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(call_subscribe, CallSubscribe1).

%% @doc Returns framework info obj.
%% @private
-spec framework_info_obj(erl_mesos:framework_info()) ->
    erl_mesos_obj:data_obj().
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
-spec framework_id_obj(undefined | erl_mesos:framework_id()) ->
    erl_mesos_obj:data_obj().
framework_id_obj(undefined) ->
    undefined;
framework_id_obj(FrameworkId) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(framework_id, FrameworkId).

%% @doc Returns framework info capabilitie obj.
%% @private
-spec framework_info_capabilitie_obj(undefined |
                                     erl_mesos:framework_info_capabilitie()) ->
    undefined | erl_mesos_obj:data_obj().
framework_info_capabilitie_obj(undefined) ->
    undefined;
framework_info_capabilitie_obj(FrameworkInfoCapabilitie) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(framework_info_capabilitie,
                               FrameworkInfoCapabilitie).

%% @doc Returns labels obj.
%% @private
-spec labels_obj(undefined | erl_mesos:labels()) -> erl_mesos_obj:data_obj().
labels_obj(undefined) ->
    undefined;
labels_obj(#labels{labels = LabelsList} = Labels) ->
    LabelObjs = [?ERL_MESOS_OBJ_FROM_RECORD(label, Label) ||
                 Label <- LabelsList],
    Labels1 = Labels#labels{labels = LabelObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(labels, Labels1).

%% @doc Returns call accept obj.
%% @private
-spec call_accept_obj(erl_mesos:call_accept()) -> erl_mesos_obj:data_obj().
call_accept_obj(#call_accept{offer_ids = OfferIds,
                             operations = Operations,
                             filters = Filters} = CallAccept) ->
    OfferIdObjs = offer_id_objs(OfferIds),
    OperationObjs = offer_operation_objs(Operations),
    FiltersObj = filters_obj(Filters),
    CallAccept1 = CallAccept#call_accept{offer_ids = OfferIdObjs,
                                         operations = OperationObjs,
                                         filters = FiltersObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(call_accept, CallAccept1).

%% @doc Returns offer id objs.
%% @private
-spec offer_id_objs(undefined | [erl_mesos:offer_id()]) ->
    undefined | [erl_mesos_obj:data_obj()].
offer_id_objs(undefined) ->
    undefined;
offer_id_objs(OfferIds) ->
    [?ERL_MESOS_OBJ_FROM_RECORD(offer_id, OfferId) || OfferId <- OfferIds].

%% @doc Returns offer operation objs.
%% @private
-spec offer_operation_objs(undefined | [erl_mesos:offer_operation()]) ->
    undefined | [erl_mesos_obj:data_obj()].
offer_operation_objs(undefined) ->
    undefined;
offer_operation_objs(OfferOperations) ->
    [offer_operation_obj(OfferOperation) || OfferOperation <- OfferOperations].

%% @doc Returns offer operation obj.
%% @private
-spec offer_operation_obj(erl_mesos:offer_operation()) ->
    erl_mesos_obj:data_obj().
offer_operation_obj(#offer_operation{launch = Launch,
                                     reserve = Reserve,
                                     unreserve = Unreserve,
                                     create = Create,
                                     destroy = Destroy} = OfferOperation) ->
    LaunchObj = offer_operation_launch_obj(Launch),
    ReserveObj = offer_operation_reserve_obj(Reserve),
    UnreserveObj = offer_operation_unreserve(Unreserve),
    CreateObj = offer_operation_create_obj(Create),
    DestroyObj = offer_operation_destroy_obj(Destroy),
    OfferOperation1 = OfferOperation#offer_operation{launch = LaunchObj,
                                                     reserve = ReserveObj,
                                                     unreserve = UnreserveObj,
                                                     create = CreateObj,
                                                     destroy = DestroyObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(offer_operation, OfferOperation1).

%% @doc Returns offer operation launch obj.
%% @private
-spec offer_operation_launch_obj(undefined |
                                 erl_mesos:offer_operation_launch()) ->
    undefined | erl_mesos_obj:data_obj().
offer_operation_launch_obj(undefined) ->
    undefined;
offer_operation_launch_obj(#offer_operation_launch{task_infos = TaskInfos} =
                           OfferOperationLaunch) ->
    TaskInfoObjs = [task_info_obj(TaskInfo) || TaskInfo <- TaskInfos],
    OfferOperationLaunch1 =
        OfferOperationLaunch#offer_operation_launch{task_infos = TaskInfoObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(offer_operation_launch, OfferOperationLaunch1).

%% @doc Returns offer operation reserve obj.
%% @private
-spec offer_operation_reserve_obj(undefined |
                                  erl_mesos:offer_operation_reserve()) ->
    undefined | erl_mesos_obj:data_obj().
offer_operation_reserve_obj(undefined) ->
    undefined;
offer_operation_reserve_obj(#offer_operation_reserve{resources = Resources} =
                            OfferOperationReserve) ->
    ResourceObjs = resource_objs(Resources),
    OfferOperationReserve1 =
        OfferOperationReserve#offer_operation_reserve{resources = ResourceObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(offer_operation_reserve, OfferOperationReserve1).

%% @doc Returns offer operation unreserve obj.
%% @private
-spec offer_operation_unreserve(undefined |
                                erl_mesos:offer_operation_unreserve()) ->
    undefined | erl_mesos_obj:data_obj().
offer_operation_unreserve(undefined) ->
    undefined;
offer_operation_unreserve(#offer_operation_unreserve{resources = Resources} =
                          OfferOperationUnreserve) ->
    ResourceObjs = resource_objs(Resources),
    OfferOperationUnreserve1 =
        OfferOperationUnreserve#offer_operation_unreserve{resources =
                                                              ResourceObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(offer_operation_reserve,
                               OfferOperationUnreserve1).

%% @doc Returns offer operation create obj.
%% @private
-spec offer_operation_create_obj(undefined |
                                 erl_mesos:offer_operation_create()) ->
    undefined | erl_mesos_obj:data_obj().
offer_operation_create_obj(undefined) ->
    undefined;
offer_operation_create_obj(#offer_operation_create{volumes = Volumes} =
                           OfferOperationCreate) ->
    VolumeObjs = resource_objs(Volumes),
    OfferOperationCreate1 =
        OfferOperationCreate#offer_operation_create{volumes = VolumeObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(offer_operation_create, OfferOperationCreate1).

%% @doc Returns offer operation destroy obj.
%% @private
-spec offer_operation_destroy_obj(undefined |
                                  erl_mesos:offer_operation_destroy()) ->
    undefined | erl_mesos_obj:data_obj().
offer_operation_destroy_obj(undefined) ->
    undefined;
offer_operation_destroy_obj(#offer_operation_destroy{volumes = Volumes} =
                            OfferOperationDestroy) ->
    VolumeObjs = resource_objs(Volumes),
    OfferOperationDestroy1 =
        OfferOperationDestroy#offer_operation_destroy{volumes = VolumeObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(offer_operation_create, OfferOperationDestroy1).

%% @doc Returns task info obj.
%% @private
-spec task_info_obj(erl_mesos:task_info()) -> erl_mesos_obj:data_obj().
task_info_obj(#task_info{task_id = TaskId,
                         agent_id = AgentId,
                         resources = Resources,
                         executor = Executor,
                         command = Command,
                         container = Container,
                         health_check = HealthCheck,
                         labels = Labels,
                         discovery = Discovery} = TaskInfo) ->
    TaskIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(task_id, TaskId),
    AgentIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(agent_id, AgentId),
    ResourceObjs = resource_objs(Resources),
    ExecutorObj = executor_info_obj(Executor),
    CommandObj = command_info_obj(Command),
    ContainerObj = container_info_obj(Container),
    HealthCheckObj = health_check_obj(HealthCheck),
    LabelsObj = labels_obj(Labels),
    DiscoveryObj = discovery_info_obj(Discovery),
    TaskInfo1 = TaskInfo#task_info{task_id = TaskIdObj,
                                   agent_id = AgentIdObj,
                                   resources = ResourceObjs,
                                   executor = ExecutorObj,
                                   command = CommandObj,
                                   container = ContainerObj,
                                   health_check = HealthCheckObj,
                                   labels = LabelsObj,
                                   discovery = DiscoveryObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(task_info, TaskInfo1).

%% @doc Returns resource objs.
%% @private
-spec resource_objs(undefined | [erl_mesos:resource()]) ->
    undefined | [erl_mesos_obj:data_obj()].
resource_objs(undefined) ->
    undefined;
resource_objs(Resources) ->
    [resource_obj(Resource) || Resource <- Resources].

%% @doc Returns resource obj.
%% @private
-spec resource_obj(erl_mesos:resource()) -> erl_mesos_obj:data_obj().
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
-spec value_scalar_obj(undefined | erl_mesos:value_scalar()) ->
    undefined | erl_mesos_obj:data_obj().
value_scalar_obj(undefined) ->
    undefined;
value_scalar_obj(ValueScalar) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(value_scalar, ValueScalar).

%% @doc Returns value ranges obj.
%% @private
-spec value_ranges_obj(undefined | erl_mesos:value_ranges()) ->
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
-spec value_set_obj(undefined | erl_mesos:value_set()) ->
    undefined | erl_mesos_obj:data_obj().
value_set_obj(undefined) ->
    undefined;
value_set_obj(ValueSet) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(value_set, ValueSet).

%% @doc Returns resource reservation info obj.
%% @private
-spec resource_reservation_info_obj(undefined |
                                    erl_mesos:resource_reservation_info()) ->
    undefined | erl_mesos_obj:data_obj().
resource_reservation_info_obj(undefined) ->
    undefined;
resource_reservation_info_obj(ResourceReservationInfo) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(resource_reservation_info,
                               ResourceReservationInfo).

%% @doc Returns resource disk info obj.
%% @private
-spec resource_disk_info_obj(undefined | erl_mesos:resource_disk_info()) ->
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
          erl_mesos:resource_disk_info_persistence()) ->
    undefined | erl_mesos_obj:data_obj().
resource_disk_info_persistence_obj(undefined) ->
    undefined;
resource_disk_info_persistence_obj(ResourceDiskInfoPersistence) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(resource_disk_info_persistence,
                               ResourceDiskInfoPersistence).

%% @doc Returns volume obj.
%% @private
-spec volume_obj(undefined | erl_mesos:volume()) ->
    undefined | erl_mesos_obj:data_obj().
volume_obj(undefined) ->
    undefined;
volume_obj(#volume{image = Image} = Volume) ->
    ImageObj = image_obj(Image),
    Volume1 = Volume#volume{image = ImageObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(volume, Volume1).

%% @doc Returns image obj.
%% @private
-spec image_obj(undefined | erl_mesos:image()) ->
    undefined | erl_mesos_obj:data_obj().
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
-spec image_appc_obj(undefined | erl_mesos:image_appc()) ->
    undefined | erl_mesos_obj:data_obj().
image_appc_obj(undefined) ->
    undefined;
image_appc_obj(#image_appc{labels = Labels} = ImageAppc) ->
    LabelsObj = labels_obj(Labels),
    ImageAppc1 = ImageAppc#image_appc{labels = LabelsObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(image_appc, ImageAppc1).

%% @doc Returns image docker obj.
%% @private
-spec image_docker_obj(undefined | erl_mesos:image_docker()) ->
    undefined | erl_mesos_obj:data_obj().
image_docker_obj(undefined) ->
    undefined;
image_docker_obj(ImageDocker) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(image_docker, ImageDocker).

%% @doc Returns resource revocable info obj.
%% @private
-spec resource_revocable_info_obj(undefined |
                                  erl_mesos:resource_revocable_info()) ->
    undefined | erl_mesos_obj:data_obj().
resource_revocable_info_obj(undefined) ->
    undefined;
resource_revocable_info_obj(ResourceRevocableInfo) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(resource_revocable_info, ResourceRevocableInfo).

%% @doc Returns executor info obj.
%% @private
-spec executor_info_obj(undefined | erl_mesos:executor_info()) ->
    undefined | erl_mesos_obj:data_obj().
executor_info_obj(undefined) ->
    undefined;
executor_info_obj(#executor_info{executor_id = ExecutorId,
                                 framework_id = FrameworkId,
                                 command = Command,
                                 container = Container,
                                 resources = Resources,
                                 discovery = Discovery} = ExecutorInfo) ->
    ExecutorIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(executor_id, ExecutorId),
    FrameworkIdObj = framework_id_obj(FrameworkId),
    CommandObj = command_info_obj(Command),
    ContainerObj = container_info_obj(Container),
    ResourceObjs = resource_objs(Resources),
    DiscoveryObj = discovery_info_obj(Discovery),
    ExecutorInfo1 = ExecutorInfo#executor_info{executor_id = ExecutorIdObj,
                                               framework_id = FrameworkIdObj,
                                               command = CommandObj,
                                               container = ContainerObj,
                                               resources = ResourceObjs,
                                               discovery = DiscoveryObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(executor_info, ExecutorInfo1).

%% @doc Returns command info obj.
%% @private
-spec command_info_obj(undefined | erl_mesos:command_info()) ->
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
          erl_mesos:command_info_container_info()) ->
    undefined | erl_mesos_obj:data_obj().
command_info_container_info_obj(undefined) ->
    undefined;
command_info_container_info_obj(CommandInfoContainerInfo) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(command_info_container_info,
                               CommandInfoContainerInfo).

%% @doc Returns command info uri objs.
%% @private
-spec command_info_uri_objs(undefined | [erl_mesos:command_info_uri()]) ->
    undefined | [erl_mesos_obj:data_obj()].
command_info_uri_objs(undefined) ->
    undefined;
command_info_uri_objs(CommandInfoUris) ->
    [?ERL_MESOS_OBJ_FROM_RECORD(command_info_uri, CommandInfoUri) ||
     CommandInfoUri <- CommandInfoUris].

%% @doc Returns environment obj.
%% @private
-spec environment_obj(undefined | erl_mesos:environment()) ->
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
-spec container_info_obj(undefined | erl_mesos:container_info()) ->
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
-spec volume_objs(undefined | [erl_mesos:volume()]) ->
    undefined | [erl_mesos_obj:data_obj()].
volume_objs(undefined) ->
    undefined;
volume_objs(Volumes) ->
    [volume_obj(Volume) || Volume <- Volumes].

%% @doc Returns container info docker info obj.
%% @private
-spec container_info_docker_info_obj(undefined |
                                     erl_mesos:container_info_docker_info()) ->
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
  [erl_mesos:container_info_docker_info_port_mapping()]) ->
    undefined | [erl_mesos_obj:data_obj()].
container_info_docker_info_port_mapping_objs(undefined) ->
    undefined;
container_info_docker_info_port_mapping_objs(PortMappings) ->
    [?ERL_MESOS_OBJ_FROM_RECORD(container_info_docker_info_port_mapping,
                                PortMapping) ||
     PortMapping <- PortMappings].

%% @doc Returns container info mesos info obj.
%% @private
-spec container_info_mesos_info_obj(undefined |
                                    erl_mesos:container_info_mesos_info()) ->
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
-spec network_info_objs(undefined | [erl_mesos:network_info()]) ->
    undefined | [erl_mesos_obj:data_obj()].
network_info_objs(undefined) ->
    undefined;
network_info_objs(NetworkInfos) ->
    [network_info_obj(NetworkInfo) || NetworkInfo <- NetworkInfos].

%% @doc Returns network info obj.
%% @private
-spec network_info_obj(erl_mesos:network_info()) -> erl_mesos_obj:data_obj().
network_info_obj(#network_info{ip_addresses = IpAddresses,
                               labels = Labels} = NetworkInfo) ->
    IpAddressObjs = network_info_ip_address_objs(IpAddresses),
    LabelsObj = labels_obj(Labels),
    NetworkInfo1 = NetworkInfo#network_info{ip_addresses = IpAddressObjs,
                                            labels = LabelsObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(network_info, NetworkInfo1).

%% @doc Returns network info ip address objs.
%% @private
-spec network_info_ip_address_objs(undefined |
                                   [erl_mesos:network_info_ip_address()]) ->
    undefined | [erl_mesos_obj:data_obj()].
network_info_ip_address_objs(undefined) ->
    undefined;
network_info_ip_address_objs(NetworkInfoIpAddresses) ->
    [?ERL_MESOS_OBJ_FROM_RECORD(network_info_ip_address, NetworkInfoIpAddress)
     || NetworkInfoIpAddress <- NetworkInfoIpAddresses].

%% @doc Returns parameter objs.
%% @private
-spec parameter_objs(undefined | [erl_mesos:parameter()]) ->
    undefined | [erl_mesos_obj:data_obj()].
parameter_objs(undefined) ->
    undefined;
parameter_objs(Parameters) ->
    [?ERL_MESOS_OBJ_FROM_RECORD(parameter, Parameter) ||
     Parameter <- Parameters].

%% @doc Returns discovery info obj.
%% @private
-spec discovery_info_obj(undefined | erl_mesos:discovery_info()) ->
    undefined | erl_mesos_obj:data_obj().
discovery_info_obj(undefined) ->
    undefined;
discovery_info_obj(#discovery_info{ports = Ports,
                                   labels = Labels} = DiscoveryInfo) ->
    PortsObj = ports_obj(Ports),
    LabelsObj = labels_obj(Labels),
    DiscoveryInfo1 = DiscoveryInfo#discovery_info{ports = PortsObj,
                                                  labels = LabelsObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(discovery_info, DiscoveryInfo1).

%% @doc Returns ports obj.
%% @private
-spec ports_obj(undefined | erl_mesos:pts()) ->
    undefined | erl_mesos_obj:data_obj().
ports_obj(undefined) ->
    undefined;
ports_obj(#ports{ports = PortsList} = Ports) ->
    PortObjs = [?ERL_MESOS_OBJ_FROM_RECORD(port, Port) || Port <- PortsList],
    Ports1 = Ports#ports{ports = PortObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(ports, Ports1).

%% @doc Returns health check obj.
%% @private
-spec health_check_obj(undefined | erl_mesos:health_check()) ->
    undefined | erl_mesos_obj:data_obj().
health_check_obj(undefined) ->
    undefined;
health_check_obj(#health_check{http = Http,
                               command = Command} = HealthCheckObj) ->
    HttpObj = health_check_http_obj(Http),
    CommandObj = command_info_obj(Command),
    HealthCheckObj1 = HealthCheckObj#health_check{http = HttpObj,
                                                  command = CommandObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(health_check, HealthCheckObj1).

%% @doc Returns health check http obj.
%% @private
-spec health_check_http_obj(undefined |
                            erl_mesos:health_check_http()) ->
    undefined | erl_mesos_obj:data_obj().
health_check_http_obj(undefined) ->
    undefined;
health_check_http_obj(HealthCheckHttpObj) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(health_check_http, HealthCheckHttpObj).

%% @doc Returns filters obj.
%% @private
-spec filters_obj(undefined | erl_mesos:filters()) ->
    undefined | erl_mesos_obj:data_obj().
filters_obj(undefined) ->
    undefined;
filters_obj(FiltersObj) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(health_check_http, FiltersObj).

%% @doc Returns call decline obj.
%% @private
-spec call_decline_obj(erl_mesos:call_decline()) ->
    erl_mesos_obj:data_obj().
call_decline_obj(#call_decline{offer_ids = OfferIds,
                               filters = Filters} = CallDecline) ->
    OfferIdObjs = offer_id_objs(OfferIds),
    FiltersObj = filters_obj(Filters),
    CallDecline1 = CallDecline#call_decline{offer_ids = OfferIdObjs,
                                            filters = FiltersObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(call_decline, CallDecline1).

%% @doc Returns call kill obj.
%% @private
-spec call_kill_obj(erl_mesos:call_kill()) -> erl_mesos_obj:data_obj().
call_kill_obj(#call_kill{task_id = TaskId,
                         agent_id = AgentId} = CallKill) ->
    TaskIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(task_id, TaskId),
    AgentIdObj = agent_id_obj(AgentId),
    CallKill1 = CallKill#call_kill{task_id = TaskIdObj, agent_id = AgentIdObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(call_kill, CallKill1).

%% @doc Returns call shutdown obj.
%% @private
-spec call_shutdown_obj(erl_mesos:call_shutdown()) -> erl_mesos_obj:data_obj().
call_shutdown_obj(#call_shutdown{executor_id = ExecutorId,
                                 agent_id = AgentId} = CallShutdown) ->
    ExecutorIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(executor_id, ExecutorId),
    AgentIdObj = agent_id_obj(AgentId),
    CallShutdown1 = CallShutdown#call_shutdown{executor_id = ExecutorIdObj,
                                               agent_id = AgentIdObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(call_shutdown, CallShutdown1).

%% @doc Returns call acknowledge obj.
%% @private
-spec call_acknowledge_obj(erl_mesos:call_acknowledge()) ->
    erl_mesos_obj:data_obj().
call_acknowledge_obj(#call_acknowledge{agent_id = AgentId,
                                       task_id = TaskId} = CallAcknowledge) ->
    AgentIdObj = agent_id_obj(AgentId),
    TaskIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(task_id, TaskId),
    CallAcknowledge1 = CallAcknowledge#call_acknowledge{agent_id = AgentIdObj,
                                                        task_id = TaskIdObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(call_acknowledge, CallAcknowledge1).

%% @doc Returns call reconcile obj.
%% @private
-spec call_reconcile_obj(erl_mesos:call_reconcile()) ->
    erl_mesos_obj:data_obj().
call_reconcile_obj(#call_reconcile{tasks = Tasks} = CallReconcile) ->
    TaskObjs = [call_reconcile_task_obj(Task) || Task <- Tasks],
    CallReconcile1 = CallReconcile#call_reconcile{tasks = TaskObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(call_reconcile, CallReconcile1).

%% @doc Returns call reconcile task obj.
%% @private
-spec call_reconcile_task_obj(erl_mesos:call_reconcile_task()) ->
    erl_mesos_obj:data_obj().
call_reconcile_task_obj(#call_reconcile_task{task_id = TaskId,
                                             agent_id = AgentId} =
                        CallReconcileTask) ->
    TaskIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(task_id, TaskId),
    AgentIdObj = agent_id_obj(AgentId),
    CallReconcileTask1 =
        CallReconcileTask#call_reconcile_task{task_id = TaskIdObj,
                                              agent_id = AgentIdObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(call_reconcile_task, CallReconcileTask1).

%% @doc Returns agent id obj.
%% @private
-spec agent_id_obj(undefined | erl_mesos:agent_id()) ->
    undefined | erl_mesos_obj:data_obj().
agent_id_obj(undefined) ->
    undefined;
agent_id_obj(AgentId) ->
    ?ERL_MESOS_OBJ_FROM_RECORD(agent_id, AgentId).

%% @doc Returns call message obj.
%% @private
-spec call_message_obj(erl_mesos:call_message()) ->
    erl_mesos_obj:data_obj().
call_message_obj(#call_message{agent_id = AgentId,
                               executor_id = ExecutorId} = CallMessage) ->
    AgentIdObj = agent_id_obj(AgentId),
    ExecutorIdObj = ?ERL_MESOS_OBJ_FROM_RECORD(executor_id, ExecutorId),
    CallMessage1 = CallMessage#call_message{agent_id = AgentIdObj,
                                            executor_id = ExecutorIdObj},
    ?ERL_MESOS_OBJ_FROM_RECORD(call_acknowledge, CallMessage1).

%% @doc Returns call request obj.
%% @private
-spec call_request_obj(erl_mesos:call_request()) -> erl_mesos_obj:data_obj().
call_request_obj(#call_request{requests = Requests} = CallRequest) ->
    RequestObjs = [request_obj(Request) || Request <- Requests],
    CallRequest1 = CallRequest#call_request{requests = RequestObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(call_request, CallRequest1).

%% @doc Returns request obj.
%% @private
-spec request_obj(erl_mesos:request()) -> erl_mesos_obj:data_obj().
request_obj(#request{agent_id = AgentId, resources = Resources} = Request) ->
    AgentIdObj = agent_id_obj(AgentId),
    ResourceObjs = resource_objs(Resources),
    Request1 = Request#request{agent_id = AgentIdObj, resources = ResourceObjs},
    ?ERL_MESOS_OBJ_FROM_RECORD(request, Request1).


%% @doc Sends http request.
%% @private
-spec send_request(erl_mesos:scheduler_info(), erl_mesos_obj:data_obj()) ->
    {ok, erl_mesos_http:client_ref()} |
    {ok, non_neg_integer(), erl_mesos_http:headers(),
     erl_mesos_http:client_ref()} |
    {error, term()}.
send_request(#scheduler_info{data_format = DataFormat,
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
