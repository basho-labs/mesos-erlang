-module(erl_mesos_utils).

-include("scheduler_protobuf.hrl").

-include("utils.hrl").

-export([resources_cpus/1,
         resources_mem/1,
         resources_disk/1,
         resources_ports/1,
         extract_resources/1]).

-export([command_info_uri/1, command_info_uri/2, command_info_uri/3]).

-export([command_info/1, command_info/2, command_info/3, command_info/4]).

-export([scalar_resource/2, ranges_resource/2, set_resource/2]).

-export([executor_id/1]).

-export([executor_info/2, executor_info/3, executor_info/4]).

-export([framework_id/1]).

-export([framework_info/2, framework_info/3]).

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

%% @equiv command_info(Value, Uris, true, undefined)
-spec command_info(string(), [erl_mesos:'CommandInfo.URI'()]) ->
    erl_mesos:'CommandInfo'().
command_info(Value, Uris) ->
    command_info(Value, Uris, true, undefined).

%% @equiv command_info(Value, Uris, Shell, undefined)
-spec command_info(string(), [erl_mesos:'CommandInfo.URI'()], boolean()) ->
    erl_mesos:'CommandInfo'().
command_info(Value, Uris, Shell) ->
    command_info(Value, Uris, Shell, undefined).

%% @doc Returns command info.
-spec command_info(string(), [erl_mesos:'CommandInfo.URI'()], boolean(),
                   undefined | string()) ->
    erl_mesos:'CommandInfo'().
command_info(Value, Uris, Shell, User) ->
    #'CommandInfo'{uris = Uris,
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

%% @doc Returns executor id.
-spec executor_id(string()) -> erl_mesos:'ExecutorID'().
executor_id(Value) ->
    #'ExecutorID'{value = Value}.

%% @equiv executor_info(Id, Command, [], undefined)
-spec executor_info(erl_mesos:'ExecutorID'(), erl_mesos:'CommandInfo'()) ->
    erl_mesos:'ExecutorInfo'().
executor_info(Id, Command) ->
    executor_info(Id, Command, [], undefined).

%% @equiv executor_info(Id, Command, Resources, undefined)
-spec executor_info(erl_mesos:'ExecutorID'(), erl_mesos:'CommandInfo'(),
                    [erl_mesos:'Resource'()]) ->
    erl_mesos:'ExecutorInfo'().
executor_info(Id, Command, Resources) ->
    executor_info(Id, Command, Resources, undefined).

%% @doc Returns executor info.
-spec executor_info(erl_mesos:'ExecutorID'(), erl_mesos:'CommandInfo'(),
                    [erl_mesos:'Resource'()],
                    undefined | erl_mesos:'FrameworkID'()) ->
    erl_mesos:'ExecutorInfo'().
executor_info(Id, Command, Resources, FrameworkId) ->
    #'ExecutorInfo'{executor_id = Id,
                    framework_id = FrameworkId,
                    command = Command,
                    resources = Resources}.

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

