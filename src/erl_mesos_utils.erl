-module(erl_mesos_utils).

-include("scheduler_protobuf.hrl").

-include("utils.hrl").

-export([extract_resources/1]).

-export([command_info/1,
         command_info_uri/3]).

-type resources() :: #resources{}.
-export_type([resources/0]).

%% External functions.

%% @doc Returns extracted resources.
-spec extract_resources([erl_mesos:'Resource'()]) -> resources().
extract_resources(Resources) ->
    extract_resources(Resources, #resources{}).

%% @doc Returns command info uri.
-spec command_info_uri(string(), boolean(), boolean()) ->
    erl_mesos:'CommandInfo.URI'().
command_info_uri(Value, Extract, Executable) ->
    #'CommandInfo.URI'{value = Value,
                       extract = Extract,
                       executable = Executable}.

%% @doc Returns command info.
-spec command_info(string()) -> erl_mesos:'CommandInfo'().
command_info(Value) ->
    #'CommandInfo'{value = Value}.

%% Internal functions.

%% @doc Returns extracted resources.
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
    Ports1 = Ports ++ value_ranges_to_list(Ranges, []),
    extract_resources(Resources, Res#resources{ports = Ports1});
extract_resources([], Res) ->
    Res.

value_ranges_to_list([#'Value.Range'{'begin' = Begin, 'end' = End} | Ranges],
                     Values) ->
    value_ranges_to_list(Ranges, Values ++ lists:seq(Begin, End));
value_ranges_to_list([], Values) ->
    Values.
