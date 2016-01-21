-module(erl_mesos_utils_SUITE).

-include_lib("common_test/include/ct.hrl").

-include_lib("scheduler_protobuf.hrl").

-export([all/0]).

-export([extract_resources/1,
         command_info_uri/1,
         command_info/1,
         resource/1]).

all() ->
    [extract_resources,
     command_info_uri,
     command_info,
     resource].

%% Test functions.

extract_resources(_Config) ->
    Res = erl_mesos_utils:extract_resources([]),
    0.0 = erl_mesos_utils:resources_cpus(Res),
    0.0 = erl_mesos_utils:resources_mem(Res),
    0.0 = erl_mesos_utils:resources_disk(Res),
    [] = erl_mesos_utils:resources_ports(Res),
    CpusValue1 = 0.1,
    CpusValue2 = 0.2,
    MemValue1 = 0.3,
    MemValue2= 0.4,
    DiskValue1 = 128.0,
    DiskValue2 = 256.0,
    PortRanges1 = [#'Value.Range'{'begin' = 0, 'end' = 3},
                   #'Value.Range'{'begin' = 4, 'end' = 6}],
    PortRanges2 = [#'Value.Range'{'begin' = 7, 'end' = 9}],
    Resourses = [#'Resource'{name = "cpus",
                             type = 'SCALAR',
                             scalar = #'Value.Scalar'{value = CpusValue1}},
                 #'Resource'{name = "cpus",
                             type = 'SCALAR',
                             scalar = #'Value.Scalar'{value = CpusValue2}},
                 #'Resource'{name = "mem",
                             type = 'SCALAR',
                             scalar = #'Value.Scalar'{value = MemValue1}},
                 #'Resource'{name = "mem",
                             type = 'SCALAR',
                             scalar = #'Value.Scalar'{value = MemValue2}},
                 #'Resource'{name = "disk",
                             type = 'SCALAR',
                             scalar = #'Value.Scalar'{value = DiskValue1}},
                 #'Resource'{name = "disk",
                             type = 'SCALAR',
                             scalar = #'Value.Scalar'{value = DiskValue2}},
                 #'Resource'{name = "ports",
                             type = 'RANGES',
                             ranges = #'Value.Ranges'{range = PortRanges1}},
                 #'Resource'{name = "ports",
                             type = 'RANGES',
                             ranges = #'Value.Ranges'{range = PortRanges2}}],
    Res1 = erl_mesos_utils:extract_resources(Resourses),
    Cpus = erl_mesos_utils:resources_cpus(Res1),
    Mem = erl_mesos_utils:resources_mem(Res1),
    Disk = erl_mesos_utils:resources_disk(Res1),
    Ports = erl_mesos_utils:resources_ports(Res1),
    Cpus = CpusValue1 + CpusValue2,
    Mem = MemValue1 + MemValue2,
    Disk = DiskValue1 + DiskValue2,
    Ports = lists:seq(0, 9).

command_info_uri(_Config) ->
    Value = "value",
    #'CommandInfo.URI'{value = Value,
                       executable = true,
                       extract = false} =
        erl_mesos_utils:command_info_uri(Value),
    #'CommandInfo.URI'{value = Value,
                       executable = false,
                       extract = false} =
        erl_mesos_utils:command_info_uri(Value, false),
    #'CommandInfo.URI'{value = Value,
                       executable = true,
                       extract = false} =
        erl_mesos_utils:command_info_uri(Value, true, false).

command_info(_Config) ->
    Value = "value",
    CommandInfoUri = erl_mesos_utils:command_info_uri(Value),
    #'CommandInfo'{uris = [],
                   shell = true,
                   value = Value} = erl_mesos_utils:command_info(Value),
    #'CommandInfo'{uris = [CommandInfoUri],
                   shell = true,
                   value = Value} =
        erl_mesos_utils:command_info(Value, [CommandInfoUri]),
    #'CommandInfo'{uris = [CommandInfoUri],
                   shell = false,
                   value = Value} =
        erl_mesos_utils:command_info(Value, [CommandInfoUri], false).

resource(_Config) ->
    ScalarName = "cpus",
    ScalarValue = 0.1,
    RangesName = "ports",
    Ranges = [{1, 3}, {4, 6}],
    ValueRanges = [#'Value.Range'{'begin' = Begin, 'end' = End} ||
                   {Begin, End} <- Ranges],
    #'Resource'{name = ScalarName,
                type = 'SCALAR',
                scalar = #'Value.Scalar'{value = ScalarValue}} =
        erl_mesos_utils:scalar_resource(ScalarName, ScalarValue),
    #'Resource'{name = RangesName,
                type = 'RANGES',
                ranges = #'Value.Ranges'{range = ValueRanges}} =
            erl_mesos_utils:ranges_resource(RangesName, Ranges),
    ok.
