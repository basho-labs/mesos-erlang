-module(erl_mesos_utils).

-include("scheduler_protobuf.hrl").

-include("utils.hrl").

-export([resources_cpus/1,
         resources_mem/1,
         resources_disk/1,
         resources_ports/1,
         extract_resources/1]).

-export([command_info_uri/1, command_info_uri/2, command_info_uri/3]).

-export([command_info/1, command_info/2, command_info/3]).

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

%% @equiv command_info(Value, [], true)
-spec command_info(string()) -> erl_mesos:'CommandInfo'().
command_info(Value) ->
    command_info(Value, [], true).

%% @equiv command_info(Value, Uris, true)
-spec command_info(string(), [erl_mesos:'CommandInfo.URI'()]) ->
    erl_mesos:'CommandInfo'().
command_info(Value, Uris) ->
    command_info(Value, Uris, true).

%% @doc Returns command info.
-spec command_info(string(), [erl_mesos:'CommandInfo.URI'()], boolean()) ->
    erl_mesos:'CommandInfo'().
command_info(Value, Uris, Shell) ->
    #'CommandInfo'{uris = Uris,
                   shell = Shell,
                   value = Value}.

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

