-module(erl_mesos_utils).

-include("scheduler_protobuf.hrl").

-include("utils.hrl").

-export([resources_cpus/1,
         resources_mem/1,
         resources_disk/1,
         resources_ports/1,
         extract_resources/1]).

-export([framework_id/1]).

-export([framework_info/2, framework_info/3]).

-export([command_info_uri/1, command_info_uri/2, command_info_uri/3]).

-export([command_info/1, command_info/2, command_info/3, command_info/4]).

-export([scalar_resource/2,
         ranges_resource/2,
         set_resource/2,
         volume_resource/4,
         add_resource_reservation/3,
         scalar_resource_reservation/4,
         ranges_resource_reservation/4,
         set_resource_reservation/4,
         volume_resource_reservation/6]).

-export([executor_id/1]).

-export([executor_info/2, executor_info/3, executor_info/4]).

-export([task_id/1, agent_id/1]).

-export([task_info/4, task_info/5, task_info/6]).

-export([launch_offer_operation/1,
         reserve_offer_operation/1,
         unreserve_offer_operation/1,
         create_offer_operation/1,
         destroy_offer_operation/1]).

-type resources() :: #resources{}.
-export_type([resources/0]).

%% External functions.

%% @doc Returns resources cpus.
-spec resources_cpus(resources()) -> float().
resources_cpus(#resources{cpus = Cpus}) ->
    Cpus.

%% @doc Returns resources mem.
-spec resources_mem(resources()) -> float().
resources_mem(#resources{mem = Mem}) ->
    Mem.

%% @doc Returns resources disk.
-spec resources_disk(resources()) -> float().
resources_disk(#resources{disk = Disk}) ->
    Disk.

%% @doc Returns resources ports.
-spec resources_ports(resources()) -> [non_neg_integer()].
resources_ports(#resources{ports = Ports}) ->
    Ports.

%% @doc Returns extracted resources.
-spec extract_resources([erl_mesos:'Resource'()]) -> resources().
extract_resources(Resources) ->
    extract_resources(Resources, #resources{}).

%% @doc Returns framework id.
-spec framework_id(string()) -> erl_mesos:'FrameworkID'().
framework_id(Value) ->
    #'FrameworkID'{value = Value}.

%% @equiv framework_info(Name, User, 0.0)
-spec framework_info(string(), string()) -> erl_mesos:'FrameworkInfo'().
framework_info(Name, User) ->
    framework_info(Name, User, 0.0).

%% @doc Returns framework info.
-spec framework_info(string(), string(), float()) ->
    erl_mesos:'FrameworkInfo'().
framework_info(Name, User, FailoverTimeout) ->
    #'FrameworkInfo'{name = Name,
                     user = User,
                     failover_timeout = FailoverTimeout}.

%% @equiv command_info_uri(Value, true, false)
-spec command_info_uri(string()) -> erl_mesos:'CommandInfo.URI'().
command_info_uri(Value) ->
    command_info_uri(Value, true, false).

%% @equiv command_info_uri(Value, Executable, false)
-spec command_info_uri(string(), boolean()) -> erl_mesos:'CommandInfo.URI'().
command_info_uri(Value, Executable) ->
    command_info_uri(Value, Executable, false).

%% @doc Returns command info uri.
-spec command_info_uri(string(), boolean(), boolean()) ->
    erl_mesos:'CommandInfo.URI'().
command_info_uri(Value, Executable, Extract) ->
    #'CommandInfo.URI'{value = Value,
                       executable = Executable,
                       extract = Extract}.

%% @equiv command_info(Value, [], true, undefined)
-spec command_info(string()) -> erl_mesos:'CommandInfo'().
command_info(Value) ->
    command_info(Value, [], true, undefined).

%% @equiv command_info(Value, CommandInfoUris, true, undefined)
-spec command_info(string(), [erl_mesos:'CommandInfo.URI'()]) ->
    erl_mesos:'CommandInfo'().
command_info(Value, CommandInfoUris) ->
    command_info(Value, CommandInfoUris, true, undefined).

%% @equiv command_info(Value, CommandInfoUris, Shell, undefined)
-spec command_info(string(), [erl_mesos:'CommandInfo.URI'()], boolean()) ->
    erl_mesos:'CommandInfo'().
command_info(Value, CommandInfoUris, Shell) ->
    command_info(Value, CommandInfoUris, Shell, undefined).

%% @doc Returns command info.
-spec command_info(string(), [erl_mesos:'CommandInfo.URI'()], boolean(),
                   undefined | string()) ->
    erl_mesos:'CommandInfo'().
command_info(Value, CommandInfoUris, Shell, User) ->
    #'CommandInfo'{uris = CommandInfoUris,
                   shell = Shell,
                   value = Value,
                   user = User}.

%% @doc Returns scalar resource.
-spec scalar_resource(string(), float()) -> erl_mesos:'Resource'().
scalar_resource(Name, Value) ->
    #'Resource'{name = Name,
                type = 'SCALAR',
                scalar = #'Value.Scalar'{value = Value}}.

%% @doc Returns ranges resource.
-spec ranges_resource(string(), [{non_neg_integer(), non_neg_integer()}]) ->
    erl_mesos:'Resource'().
ranges_resource(Name, Ranges) ->
    #'Resource'{name = Name,
                type = 'RANGES',
                ranges = #'Value.Ranges'{range =
                              [#'Value.Range'{'begin' = Begin,
                                              'end' = End} ||
                               {Begin, End} <- Ranges]}}.

%% @doc Returns ranges resource.
-spec set_resource(string(), [string()]) -> erl_mesos:'Resource'().
set_resource(Name, Items) ->
    #'Resource'{name = Name,
                type = 'SET',
                set = #'Value.Set'{item = Items}}.

%% @doc Returns ranges resource.
-spec volume_resource(float(), string(), string(), atom()) ->
    erl_mesos:'Resource'().
volume_resource(Value, PersistenceId, ContainerPath, Mode) ->
    Resource = scalar_resource("disk", Value),
    Persistence = #'Resource.DiskInfo.Persistence'{id = PersistenceId},
    Volume = #'Volume'{container_path = ContainerPath,
                       mode = Mode},
    ResourceDiskInfo = #'Resource.DiskInfo'{persistence = Persistence,
                                            volume = Volume},
    Resource#'Resource'{disk = ResourceDiskInfo}.

%% @doc Addes role and reservation info to the resource.
-spec add_resource_reservation(erl_mesos:'Resource'(), string(), string()) ->
    erl_mesos:'Resource'().
add_resource_reservation(Resource, Role, Principal) ->
    Resource#'Resource'{role = Role,
                        reservation =
                            #'Resource.ReservationInfo'{principal = Principal}}.

%% @doc Returns scalar resource with reservation.
-spec scalar_resource_reservation(string(), float(), string(), string()) ->
    erl_mesos:'Resource'().
scalar_resource_reservation(Name, Value, Role, Principal) ->
    add_resource_reservation(scalar_resource(Name, Value), Role, Principal).

%% @doc Returns ranges resource with reservation.
-spec ranges_resource_reservation(string(),
                                  [{non_neg_integer(), non_neg_integer()}],
                                  string(), string()) ->
    erl_mesos:'Resource'().
ranges_resource_reservation(Name, Ranges, Role, Principal) ->
    add_resource_reservation(ranges_resource(Name, Ranges), Role, Principal).

%% @doc Returns set resource with reservation.
-spec set_resource_reservation(string(), [string()], string(), string()) ->
    erl_mesos:'Resource'().
set_resource_reservation(Name, Items, Role, Principal) ->
    add_resource_reservation(set_resource(Name, Items), Role, Principal).

%% @doc Returns volume resource with reservation.
-spec volume_resource_reservation(float(), string(), string(), atom(), string(),
                                  string()) ->
    erl_mesos:'Resource'().
volume_resource_reservation(Value, PersistenceId, ContainerPath, Mode, Role,
                            Principal) ->
    add_resource_reservation(volume_resource(Value, PersistenceId,
                                             ContainerPath, Mode),
                             Role, Principal).

%% @doc Returns executor id.
-spec executor_id(string()) -> erl_mesos:'ExecutorID'().
executor_id(Value) ->
    #'ExecutorID'{value = Value}.

%% @equiv executor_info(ExecutorId, CommandInfo, [], undefined)
-spec executor_info(erl_mesos:'ExecutorID'(), erl_mesos:'CommandInfo'()) ->
    erl_mesos:'ExecutorInfo'().
executor_info(ExecutorId, CommandInfo) ->
    executor_info(ExecutorId, CommandInfo, [], undefined).

%% @equiv executor_info(ExecutorId, CommandInfo, Resources, undefined)
-spec executor_info(erl_mesos:'ExecutorID'(), erl_mesos:'CommandInfo'(),
                    [erl_mesos:'Resource'()]) ->
    erl_mesos:'ExecutorInfo'().
executor_info(ExecutorId, CommandInfo, Resources) ->
    executor_info(ExecutorId, CommandInfo, Resources, undefined).

%% @doc Returns executor info.
-spec executor_info(erl_mesos:'ExecutorID'(), erl_mesos:'CommandInfo'(),
                    [erl_mesos:'Resource'()],
                    undefined | erl_mesos:'FrameworkID'()) ->
    erl_mesos:'ExecutorInfo'().
executor_info(ExecutorId, CommandInfo, Resources, FrameworkId) ->
    #'ExecutorInfo'{executor_id = ExecutorId,
                    framework_id = FrameworkId,
                    command = CommandInfo,
                    resources = Resources}.

%% @doc Returns task id.
-spec task_id(string()) -> erl_mesos:'TaskID'().
task_id(Value) ->
    #'TaskID'{value = Value}.

%% @doc Returns agent id.
-spec agent_id(string()) -> erl_mesos:'AgentID'().
agent_id(Value) ->
    #'AgentID'{value = Value}.

%% @equiv task_info(Name, TaskId, AgentId, Resources, undefined, undefined)
-spec task_info(string(), erl_mesos:'TaskID'(), erl_mesos:'AgentID'(),
                [erl_mesos:'Resource'()]) ->
    erl_mesos:'TaskInfo'().
task_info(Name, TaskId, AgentId, Resources) ->
    task_info(Name, TaskId, AgentId, Resources, undefined, undefined).

%% @equiv task_info(Name, TaskId, AgentId, Resources, ExecutorInfo, undefined)
-spec task_info(string(), erl_mesos:'TaskID'(), erl_mesos:'AgentID'(),
                [erl_mesos:'Resource'()],
                undefined | erl_mesos:'ExecutorInfo'()) ->
                erl_mesos:'TaskInfo'().
task_info(Name, TaskId, AgentId, Resources, ExecutorInfo) ->
    task_info(Name, TaskId, AgentId, Resources, ExecutorInfo, undefined).

%% @doc Returns task info.
-spec task_info(string(), erl_mesos:'TaskID'(), erl_mesos:'AgentID'(),
                [erl_mesos:'Resource'()],
                undefined | erl_mesos:'ExecutorInfo'(),
                undefined | erl_mesos:'CommandInfo'()) ->
    erl_mesos:'TaskInfo'().
task_info(Name, TaskId, AgentId, Resources, ExecutorInfo, CommandInfo) ->
    #'TaskInfo'{name = Name,
                task_id = TaskId,
                agent_id = AgentId,
                resources = Resources,
                executor = ExecutorInfo,
                command = CommandInfo}.

%% @doc Returns launch offer operation.
-spec launch_offer_operation([erl_mesos:'TaskInfo'()]) ->
    erl_mesos:'Offer.Operation'().
launch_offer_operation(TaskInfos) ->
    #'Offer.Operation'{type = 'LAUNCH',
                       launch =
                           #'Offer.Operation.Launch'{task_infos = TaskInfos}}.

%% @doc Returns reserve offer operation.
-spec reserve_offer_operation([erl_mesos:'Resource'()]) ->
    erl_mesos:'Offer.Operation'().
reserve_offer_operation(Resources) ->
    #'Offer.Operation'{type = 'RESERVE',
                       reserve =
                           #'Offer.Operation.Reserve'{resources = Resources}}.

%% @doc Returns unreserve offer operation.
-spec unreserve_offer_operation([erl_mesos:'Resource'()]) ->
    erl_mesos:'Offer.Operation'().
unreserve_offer_operation(Resources) ->
    #'Offer.Operation'{type = 'UNRESERVE',
                       unreserve =
                           #'Offer.Operation.Unreserve'{resources = Resources}}.

%% @doc Returns create offer operation.
-spec create_offer_operation([erl_mesos:'Resource'()]) ->
    erl_mesos:'Offer.Operation'().
create_offer_operation(Resources) ->
    #'Offer.Operation'{type = 'CREATE',
                       create =
                           #'Offer.Operation.Create'{volumes = Resources}}.

%% @doc Returns destroy offer operation.
-spec destroy_offer_operation([erl_mesos:'Resource'()]) ->
    erl_mesos:'Offer.Operation'().
destroy_offer_operation(Resources) ->
    #'Offer.Operation'{type = 'DESTROY',
                       destroy =
                           #'Offer.Operation.Destroy'{volumes = Resources}}.

%% Internal functions.

%% @doc Returns extracted resources.
%% @private
-spec extract_resources([erl_mesos:'Resource'()], resources()) -> resources().
extract_resources([#'Resource'{name = "cpus",
                               type = 'SCALAR',
                               scalar = #'Value.Scalar'{value = Value}} |
                   Resources], #resources{cpus = Cpus} = Res) ->
    extract_resources(Resources, Res#resources{cpus = Cpus + Value});
extract_resources([#'Resource'{name = "mem",
                               type = 'SCALAR',
                               scalar = #'Value.Scalar'{value = Value}} |
                   Resources], #resources{mem = Mem} = Res) ->
    extract_resources(Resources, Res#resources{mem = Mem + Value});
extract_resources([#'Resource'{name = "disk",
                               type = 'SCALAR',
                               scalar = #'Value.Scalar'{value = Value}} |
                   Resources], #resources{disk = Disk} = Res) ->
    extract_resources(Resources, Res#resources{disk = Disk + Value});
extract_resources([#'Resource'{name = "ports",
                               type = 'RANGES',
                               ranges = #'Value.Ranges'{range = Ranges}} |
                   Resources], #resources{ports = Ports} = Res) ->
    Ports1 = lists:foldl(fun(#'Value.Range'{'begin' = Begin,
                                            'end' = End}, Acc) ->
                             Acc ++ lists:seq(Begin, End)
                         end, Ports, Ranges),
    extract_resources(Resources, Res#resources{ports = Ports1});
extract_resources([_Resource | Resources], Res) ->
    extract_resources(Resources, Res);
extract_resources([], Res) ->
    Res.

