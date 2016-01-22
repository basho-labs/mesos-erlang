%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(erl_mesos_utils_SUITE).

-include_lib("common_test/include/ct.hrl").

-include_lib("scheduler_protobuf.hrl").

-export([all/0]).

-export([framework_info/1,
         extract_resources/1,
         command_info_uri/1,
         command_info/1,
         resource/1,
         executor_info/1,
         task_info/1,
         offer_operation/1]).

all() ->
    [framework_info,
     extract_resources,
     command_info_uri,
     command_info,
     resource,
     executor_info,
     task_info,
     offer_operation].

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
    Resources = [#'Resource'{name = "cpus",
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
    Res1 = erl_mesos_utils:extract_resources(Resources),
    Cpus = erl_mesos_utils:resources_cpus(Res1),
    Mem = erl_mesos_utils:resources_mem(Res1),
    Disk = erl_mesos_utils:resources_disk(Res1),
    Ports = erl_mesos_utils:resources_ports(Res1),
    Cpus = CpusValue1 + CpusValue2,
    Mem = MemValue1 + MemValue2,
    Disk = DiskValue1 + DiskValue2,
    Ports = lists:seq(0, 9).

framework_info(_Config) ->
    Name = "name",
    User = "user",
    FailoverTimeout = 1.0,
    #'FrameworkInfo'{name = Name,
                     user = User,
                     failover_timeout = 0.0} =
        erl_mesos_utils:framework_info(Name, User),
    #'FrameworkInfo'{name = Name,
                     user = User,
                     failover_timeout = FailoverTimeout} =
        erl_mesos_utils:framework_info(Name, User, FailoverTimeout).

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
    User = "user",
    CommandInfoUri = erl_mesos_utils:command_info_uri("uri"),
    #'CommandInfo'{uris = [],
                   shell = true,
                   value = Value,
                   user = undefined} = erl_mesos_utils:command_info(Value),
    #'CommandInfo'{uris = [CommandInfoUri],
                   shell = true,
                   value = Value,
                   user = undefined} =
        erl_mesos_utils:command_info(Value, [CommandInfoUri]),
    #'CommandInfo'{uris = [CommandInfoUri],
                   shell = false,
                   value = Value,
                   user = undefined} =
        erl_mesos_utils:command_info(Value, [CommandInfoUri], false),
    #'CommandInfo'{uris = [CommandInfoUri],
                   shell = false,
                   value = Value,
                   user = User} =
        erl_mesos_utils:command_info(Value, [CommandInfoUri], false, User).

resource(_Config) ->
    ScalarName = "cpus",
    ScalarValue = 0.1,
    RangesName = "ports",
    Ranges = [{1, 3}, {4, 6}],
    SetName = "set",
    SetItems = ["frist", "second"],
    VolumeValue = 0.2,
    VolumePersistenceId = "persistence_id",
    VolumeContainerPath = "container_path",
    VolumeMode = 'RW',
    Role = "*",
    Principal = "principal",
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
    #'Resource'{name = SetName,
                type = 'SET',
                set = #'Value.Set'{item = SetItems}} =
        erl_mesos_utils:set_resource(SetName, SetItems),
    #'Resource'{name = "disk",
                type = 'SCALAR',
                scalar = #'Value.Scalar'{value = VolumeValue},
                disk =
                    #'Resource.DiskInfo'{persistence = VolumePersistence,
                                         volume = Volume}} =
        erl_mesos_utils:volume_resource(VolumeValue, VolumePersistenceId,
                                        VolumeContainerPath, VolumeMode),
    VolumePersistence = #'Resource.DiskInfo.Persistence'{id =
                                                         VolumePersistenceId},
    Volume = #'Volume'{container_path = VolumeContainerPath,
                       mode = VolumeMode},
    #'Resource'{name = ScalarName,
                type = 'SCALAR',
                scalar = #'Value.Scalar'{value = ScalarValue},
                role = Role,
                reservation = #'Resource.ReservationInfo'{principal =
                                                              Principal}} =
        erl_mesos_utils:scalar_resource_reservation(ScalarName, ScalarValue,
                                                    Role, Principal),
    #'Resource'{name = RangesName,
                type = 'RANGES',
                ranges = #'Value.Ranges'{range = ValueRanges},
                role = Role,
                reservation = #'Resource.ReservationInfo'{principal =
                                                              Principal}} =
        erl_mesos_utils:ranges_resource_reservation(RangesName, Ranges, Role,
                                                    Principal),
    #'Resource'{name = SetName,
                type = 'SET',
                set = #'Value.Set'{item = SetItems},
                role = Role,
                reservation = #'Resource.ReservationInfo'{principal =
                                                              Principal}} =
        erl_mesos_utils:set_resource_reservation(SetName, SetItems, Role,
                                                 Principal),
    #'Resource'{name = "disk",
                type = 'SCALAR',
                scalar = #'Value.Scalar'{value = VolumeValue},
                role = Role,
                reservation = #'Resource.ReservationInfo'{principal =
                                                              Principal},
                disk =
                    #'Resource.DiskInfo'{persistence = VolumePersistence,
                                         volume = Volume}} =
        erl_mesos_utils:volume_resource_reservation(VolumeValue,
                                                    VolumePersistenceId,
                                                    VolumeContainerPath,
                                                    VolumeMode, Role,
                                                    Principal).

executor_info(_Config) ->
    ExecutorId = erl_mesos_utils:executor_id("executor_id"),
    CommandInfoUri = erl_mesos_utils:command_info_uri("uri"),
    CommandInfo = erl_mesos_utils:command_info("command", [CommandInfoUri]),
    Resources = [erl_mesos_utils:scalar_resource("cpus", 0.1)],
    FrameworkId = erl_mesos_utils:framework_id("framework_id"),
    #'ExecutorInfo'{executor_id = ExecutorId,
                    framework_id = undefined,
                    command = CommandInfo,
                    resources = []} =
        erl_mesos_utils:executor_info(ExecutorId, CommandInfo),
    #'ExecutorInfo'{executor_id = ExecutorId,
                    framework_id = undefined,
                    command = CommandInfo,
                    resources = Resources} =
        erl_mesos_utils:executor_info(ExecutorId, CommandInfo, Resources),
    #'ExecutorInfo'{executor_id = ExecutorId,
                    framework_id = FrameworkId,
                    command = CommandInfo,
                    resources = Resources} =
        erl_mesos_utils:executor_info(ExecutorId, CommandInfo, Resources,
                                      FrameworkId).

task_info(_Config) ->
    Name = "name",
    TaskId = erl_mesos_utils:task_id("task_id"),
    AgentId = erl_mesos_utils:agent_id("agent_id"),
    Resources = [erl_mesos_utils:scalar_resource("cpus", 0.1)],
    CommandInfo = erl_mesos_utils:command_info("command"),
    ExecutorId = erl_mesos_utils:executor_id("executor_id"),
    ExecutorInfo = erl_mesos_utils:executor_info(ExecutorId, CommandInfo),
    #'TaskInfo'{name = Name,
                task_id = TaskId,
                agent_id = AgentId,
                resources = Resources,
                executor = ExecutorInfo,
                command = undefined} =
        erl_mesos_utils:task_info(Name, TaskId, AgentId, Resources,
                                  ExecutorInfo),
    #'TaskInfo'{name = Name,
                task_id = TaskId,
                agent_id = AgentId,
                resources = Resources,
                executor = undefined,
                command = CommandInfo} =
        erl_mesos_utils:task_info(Name, TaskId, AgentId, Resources, undefined,
                                  CommandInfo).

offer_operation(_Config) ->
    Name = "name",
    TaskId = erl_mesos_utils:task_id("task_id"),
    AgentId = erl_mesos_utils:agent_id("agent_id"),
    Resources = [erl_mesos_utils:scalar_resource("cpus", 0.1)],
    CommandInfo = erl_mesos_utils:command_info("command"),
    ExecutorId = erl_mesos_utils:executor_id("executor_id"),
    ExecutorInfo = erl_mesos_utils:executor_info(ExecutorId, CommandInfo),
    TaskInfo = erl_mesos_utils:task_info(Name, TaskId, AgentId, Resources,
                                         ExecutorInfo),
    #'Offer.Operation'{type = 'LAUNCH',
                       launch =
                           #'Offer.Operation.Launch'{task_infos = [TaskInfo]}} =
        erl_mesos_utils:launch_offer_operation([TaskInfo]),
    #'Offer.Operation'{type = 'RESERVE',
                       reserve =
                           #'Offer.Operation.Reserve'{resources =
                                                      [Resources]}} =
        erl_mesos_utils:reserve_offer_operation([Resources]),
    #'Offer.Operation'{type = 'UNRESERVE',
                       unreserve =
                           #'Offer.Operation.Unreserve'{resources =
                                                        [Resources]}} =
        erl_mesos_utils:unreserve_offer_operation([Resources]),
    #'Offer.Operation'{type = 'CREATE',
                       create =
                           #'Offer.Operation.Create'{volumes = [Resources]}} =
        erl_mesos_utils:create_offer_operation([Resources]),
    #'Offer.Operation'{type = 'DESTROY',
                       destroy =
                           #'Offer.Operation.Destroy'{volumes = [Resources]}} =
    erl_mesos_utils:destroy_offer_operation([Resources]).
