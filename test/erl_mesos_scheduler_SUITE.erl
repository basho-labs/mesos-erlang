%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License. You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(erl_mesos_scheduler_SUITE).

-include_lib("common_test/include/ct.hrl").

-include_lib("erl_mesos_scheduler_info.hrl").

-include_lib("erl_mesos_scheduler_proto.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2]).

-export([bad_options/1,
         registered/1,
         disconnected/1,
         reregistered/1,
         resource_offers/1,
         offer_rescinded/1,
         status_update/1,
         framework_message/1,
         error/1,
         teardown/1,
         accept/1,
         decline/1,
         revive/1,
         kill/1,
         shutdown/1,
         acknowledge/1,
         reconcile/1,
         request/1,
         suppress/1]).

all() ->
    [bad_options, {group, mesos_cluster, [sequence]}].

groups() ->
    [{mesos_cluster, [registered,
                      disconnected,
                      reregistered,
                      resource_offers,
                      offer_rescinded,
                      status_update,
                      framework_message,
                      error,
                      teardown,
                      accept,
                      decline,
                      revive,
                      kill,
                      shutdown,
                      acknowledge,
                      reconcile,
                      request,
                      suppress]}].

init_per_suite(Config) ->
    {ok, _Apps} = application:ensure_all_started(erl_mesos),
    Scheduler = erl_mesos_test_scheduler,
    SchedulerOptions = [{user, "root"}, {name, "erl_mesos_test_scheduler"}],
    {ok, Masters} = erl_mesos_cluster:config(masters, Config),
    MasterHosts = [MasterHost || {_Container, MasterHost} <- Masters],
    Options = [{master_hosts, MasterHosts}],
    [{scheduler, Scheduler},
     {scheduler_options, SchedulerOptions},
     {options, Options} |
     Config].

end_per_suite(_Config) ->
    ok.

init_per_group(mesos_cluster, Config) ->
    Config.

end_per_group(mesos_cluster, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    process_flag(trap_exit, true),
    case lists:member(TestCase, proplists:get_value(mesos_cluster, groups())) of
        true ->
            stop_mesos_cluster(Config),
            start_mesos_cluster(Config),
            Config;
        false ->
            Config
    end.

end_per_testcase(TestCase, Config) ->
    case lists:member(TestCase, proplists:get_value(mesos_cluster, groups())) of
        true ->
            stop_mesos_cluster(Config),
            Config;
        false ->
            Config
    end.

%% Test functions.

bad_options(Config) ->
    Name = erl_mesos_scheduler_bad_options,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    %% Bad options.
    Options = undefined,
    {error, {bad_options, Options}} =
        start_scheduler(Name, Scheduler, SchedulerOptions, Options),
    %% Bad master hosts.
    MasterHosts = undefined,
    Options1 = [{master_hosts, MasterHosts}],
    {error, {bad_master_hosts, MasterHosts}} =
        start_scheduler(Name, Scheduler, SchedulerOptions, Options1),
    MasterHosts1 = [],
    Options2 = [{master_hosts, MasterHosts1}],
    {error, {bad_master_hosts, MasterHosts1}} =
        start_scheduler(Name, Scheduler, SchedulerOptions, Options2),
    %% Bad request options.
    RequestOptions = undefined,
    Options3 = [{request_options, RequestOptions}],
    {error, {bad_request_options, RequestOptions}} =
        start_scheduler(Name, Scheduler, SchedulerOptions, Options3),
    %% Bad heartbeat timeout window.
    HeartbeatTimeoutWindow = undefined,
    Options4 = [{heartbeat_timeout_window, HeartbeatTimeoutWindow}],
    {error, {bad_heartbeat_timeout_window, HeartbeatTimeoutWindow}} =
        start_scheduler(Name, Scheduler, SchedulerOptions, Options4),
    %% Bad maximum number of resubscribe.
    MaxNumResubscribe = undefined,
    Options5 = [{max_num_resubscribe, MaxNumResubscribe}],
    {error, {bad_max_num_resubscribe, MaxNumResubscribe}} =
        start_scheduler(Name, Scheduler, SchedulerOptions, Options5),
    %% Bad resubscribe interval.
    ResubscribeInterval = undefined,
    Options6 = [{resubscribe_interval, ResubscribeInterval}],
    {error, {bad_resubscribe_interval, ResubscribeInterval}} =
        start_scheduler(Name, Scheduler, SchedulerOptions, Options6).

%% Callbacks.

registered(Config) ->
    Name = erl_mesos_scheduler_registered,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _Pid} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {_, SchedulerInfo, EventSubscribed}} = recv_reply(registered),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost,
                    subscribed = true,
                    framework_id = FrameworkId,
                    stream_id = StreamId} = SchedulerInfo,
    MasterHosts = proplists:get_value(master_hosts, Options),
    true = lists:member(binary_to_list(MasterHost), MasterHosts),
    true = is_binary(StreamId),
    %% Test event subscribed.
    #'Event.Subscribed'{framework_id = FrameworkId,
                        heartbeat_interval_seconds = HeartbeatIntervalSeconds} =
        EventSubscribed,
    true = is_float(HeartbeatIntervalSeconds),
    ok = stop_scheduler(Name).

disconnected(Config) ->
    Name = erl_mesos_scheduler_disconnected,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    Options1 = [{max_num_resubscribe, 0} | Options],
    %% Test connection crash.
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options1),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    Pid = response_pid(),
    exit(Pid, kill),
    {disconnected, {SchedulerPid, SchedulerInfo}} = recv_reply(disconnected),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost,
                    subscribed = false,
                    stream_id = undefined} = SchedulerInfo,
    MasterHosts = proplists:get_value(master_hosts, Options),
    true = lists:member(binary_to_list(MasterHost), MasterHosts),
    %% Test cluster stop.
    Name1 = erl_mesos_scheduler_disconnected_1,
    {ok, _} = start_scheduler(Name1, Scheduler, SchedulerOptions1, Options1),
    {registered, {SchedulerPid1, _, _}} = recv_reply(registered),
    erl_mesos_cluster:stop(Config),
    {disconnected, {SchedulerPid1, SchedulerInfo1}} = recv_reply(disconnected),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost1,
                    subscribed = false,
                    stream_id = undefined} = SchedulerInfo1,
    true = lists:member(binary_to_list(MasterHost1), MasterHosts).

reregistered(Config) ->
    Name = erl_mesos_scheduler_reregistered,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = [{failover_timeout, 1000} |
                         set_test_pid(SchedulerOptions)],
    Options = ?config(options, Config),
    Options1 = [{max_num_resubscribe, 2},
                {resubscribe_interval, 3000} |
                Options],
    %% Test connection crash.
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options1),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    Pid = response_pid(),
    exit(Pid, kill),
    {disconnected, {SchedulerPid, _}} = recv_reply(disconnected),
    {reregistered, {SchedulerPid, SchedulerInfo}} = recv_reply(reregistered),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost,
                    subscribed = true} = SchedulerInfo,
    MasterHosts = proplists:get_value(master_hosts, Options),
    true = lists:member(binary_to_list(MasterHost), MasterHosts),
    ok = stop_scheduler(Name),
    Name1 = erl_mesos_scheduler_reregistered_1,
    %% Test stop master.
    {ok, _} = start_scheduler(Name1, Scheduler, SchedulerOptions1, Options1),
    {registered, {SchedulerPid1, SchedulerInfo1, _}} = recv_reply(registered),
    #scheduler_info{master_host = MasterHost1} = SchedulerInfo1,
    MasterContainer = master_container(MasterHost1, Config),
    stop_mesos_master(MasterContainer, Config),
    {disconnected, {SchedulerPid1, _}} = recv_reply(disconnected),
    {reregistered, {SchedulerPid1, SchedulerInfo2}} = recv_reply(reregistered),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost2,
                    subscribed = true} = SchedulerInfo2,
    true = MasterHost2 =/= MasterHost1,
    true = lists:member(binary_to_list(MasterHost2), MasterHosts),
    ok = stop_scheduler(Name1).

resource_offers(Config) ->
    Name = erl_mesos_scheduler_resource_offers,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    {resource_offers, {SchedulerPid, SchedulerInfo, EventOffers}} =
        recv_reply(resource_offers),
    %% Test scheduler info.
    #scheduler_info{subscribed = true} = SchedulerInfo,
    %% Test event offer.
    #'Event.Offers'{offers = Offers} = EventOffers,
    [Offer | _] = Offers,
    #'Offer'{id = Id,
             framework_id = FrameworkId,
             agent_id = AgentId,
             hostname = Hostname,
             url = Url,
             resources = Resources} = Offer,
    #'OfferID'{value = OfferIdValue} = Id,
    true = is_list(OfferIdValue),
    #'FrameworkID'{value = FrameworkIdValue} = FrameworkId,
    true = is_list(FrameworkIdValue),
    #'AgentID'{value = AgentIdValue} = AgentId,
    true = is_list(AgentIdValue),
    true = is_list(Hostname),
    true = is_record(Url, 'URL'),
    ResourceFun = fun(#'Resource'{name = ResourceName,
                                  type = Type,
                                  scalar = Scalar,
                                  ranges = Ranges}) ->
                        true = is_list(ResourceName),
                        true = is_atom(Type),
                        case Type of
                            'SCALAR' ->
                                #'Value.Scalar'{value = ScalarValue} = Scalar,
                                true = is_float(ScalarValue),
                                undefined = Ranges;
                            'RANGES' ->
                                undefined = Scalar,
                                #'Value.Ranges'{range = ValueRanges} = Ranges,
                                [ValueRange | _] = ValueRanges,
                                #'Value.Range'{'begin' = ValueRangeBegin,
                                               'end' = ValueRangeEnd} =
                                    ValueRange,
                                true = is_integer(ValueRangeBegin),
                                true = is_integer(ValueRangeEnd)
                        end
                  end,
    lists:map(ResourceFun, Resources),
    Res = erl_mesos_utils:extract_resources(Resources),
    Cpus = erl_mesos_utils:resources_cpus(Res),
    Mem = erl_mesos_utils:resources_mem(Res),
    Disk = erl_mesos_utils:resources_disk(Res),
    Ports = erl_mesos_utils:resources_ports(Res),
    true = is_float(Cpus),
    true = is_float(Mem),
    true = is_float(Disk),
    true = is_list(Ports),
    ok = stop_scheduler(Name).

offer_rescinded(Config) ->
    Name = erl_mesos_scheduler_offer_rescinded,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    {resource_offers, {SchedulerPid, _, _}} = recv_reply(resource_offers),
    %% Test scheduler info.
    stop_mesos_slave(Config),
    {offer_rescinded, {SchedulerPid, SchedulerInfo, EventRescind}} =
        recv_reply(offer_rescinded),
    #scheduler_info{subscribed = true} = SchedulerInfo,
    %% Test event rescind.
    #'Event.Rescind'{offer_id = OfferId} = EventRescind,
    #'OfferID'{value = Value} = OfferId,
    true = is_list(Value),
    ok = stop_scheduler(Name).

status_update(Config) ->
    Name = erl_mesos_scheduler_status_update,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{id = OfferId, agent_id = AgentId} = Offer,
    TaskId = timestamp_task_id(),
    SchedulerPid ! {accept, OfferId, AgentId, TaskId},
    {accept, ok} = recv_reply(accept),
    {status_update, {SchedulerPid, SchedulerInfo, EventUpdate}} =
        recv_reply(status_update),
    %% Test scheduler info.
    #scheduler_info{subscribed = true} = SchedulerInfo,
    %% Test event update.
    #'Event.Update'{status = TaskStatus} = EventUpdate,
    #'TaskStatus'{task_id = TaskId,
                  state = 'TASK_RUNNING',
                  source = 'SOURCE_EXECUTOR',
                  agent_id = AgentId,
                  timestamp = Timestamp,
                  uuid = Uuid} = TaskStatus,
    true = is_float(Timestamp),
    true = is_binary(Uuid),
    ok = stop_scheduler(Name).

framework_message(Config) ->
    Name = erl_mesos_scheduler_framework_message,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{id = OfferId, agent_id = AgentId} = Offer,
    TaskId = timestamp_task_id(),
    SchedulerPid ! {accept, OfferId, AgentId, TaskId},
    {accept, ok} = recv_reply(accept),
    {status_update, {SchedulerPid, _SchedulerInfo, _EventUpdate}} =
        recv_reply(status_update),
    ExecutorId = #'ExecutorID'{value = TaskId#'TaskID'.value},
    SchedulerPid ! {message, AgentId, ExecutorId, base64:encode(<<"message">>)},
    {message, ok} = recv_reply(message),
    {framework_message, {SchedulerPid, SchedulerInfo, EventMessage}} =
        recv_reply(framework_message),
    %% Test scheduler info.
    #scheduler_info{subscribed = true} = SchedulerInfo,
    %% Test event message.
    #'Event.Message'{agent_id = AgentId, data = Data} = EventMessage,
    true = is_binary(Data),
    ok = stop_scheduler(Name).

error(Config) ->
    Name = erl_mesos_scheduler_error,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = [{failover_timeout, 1} |
                         set_test_pid(SchedulerOptions)],
    Options = ?config(options, Config),
    Options1 = [{max_num_resubscribe, 1},
                {resubscribe_interval, 1500} |
                Options],
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options1),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    Pid = response_pid(),
    exit(Pid, kill),
    {disconnected, {SchedulerPid, _}} = recv_reply(disconnected),
    {error, {SchedulerPid, _SchedulerInfo, EventError}} = recv_reply(error),
    %% Test error event.
    #'Event.Error'{message = Message} = EventError,
    true = is_list(Message),
    {terminate, {SchedulerPid, _, _, _}} = recv_reply(terminate).

%% Calls.

teardown(Config) ->
    Name = erl_mesos_scheduler_teardown,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    SchedulerPid ! teardown,
    {teardown, ok} = recv_reply(teardown),
    {terminate, {SchedulerPid, _, _, _}} = recv_reply(terminate).

accept(Config) ->
    Name = erl_mesos_scheduler_accept,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{id = OfferId, agent_id = AgentId} = Offer,
    TaskId = timestamp_task_id(),
    SchedulerPid ! {accept, OfferId, AgentId, TaskId},
    {accept, ok} = recv_reply(accept),
    {status_update, {SchedulerPid, _, EventUpdate}} = recv_reply(status_update),
    #'Event.Update'{status = Status} = EventUpdate,
    #'TaskStatus'{task_id = TaskId,
                  state = 'TASK_RUNNING',
                  agent_id = AgentId} = Status,
    ok = stop_scheduler(Name).

decline(Config) ->
    Name = erl_mesos_scheduler_decline,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{id = OfferId} = Offer,
    SchedulerPid ! {decline, OfferId},
    {decline, ok} = recv_reply(decline),
    ok = stop_scheduler(Name).

revive(Config) ->
    Name = erl_mesos_scheduler_revive,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{id = OfferId, agent_id = AgentId} = Offer,
    TaskId = timestamp_task_id(),
    SchedulerPid ! {accept, OfferId, AgentId, TaskId},
    {accept, ok} = recv_reply(accept),
    {status_update, {SchedulerPid, _, _}} = recv_reply(status_update),
    SchedulerPid ! revive,
    {revive, ok} = recv_reply(revive),
    ok = stop_scheduler(Name).

kill(Config) ->
    Name = erl_mesos_scheduler_kill,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{id = OfferId, agent_id = AgentId} = Offer,
    TaskId = timestamp_task_id(),
    SchedulerPid ! {accept, OfferId, AgentId, TaskId},
    {accept, ok} = recv_reply(accept),
    {status_update, {SchedulerPid, _, _}} = recv_reply(status_update),
    SchedulerPid ! {kill, TaskId},
    {kill, ok} = recv_reply(kill),
    ok = stop_scheduler(Name).

shutdown(Config) ->
    Name = erl_mesos_scheduler_shutdown,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{id = OfferId, agent_id = AgentId} = Offer,
    TaskId = timestamp_task_id(),
    SchedulerPid ! {accept, OfferId, AgentId, TaskId},
    {accept, ok} = recv_reply(accept),
    {status_update, {SchedulerPid, _, _EventUpdate}} =
        recv_reply(status_update),
    ExecutorId = #'ExecutorID'{value = TaskId#'TaskID'.value},
    SchedulerPid ! {shutdown, ExecutorId, AgentId},
    {shutdown, ok} = recv_reply(shutdown),
    ok = stop_scheduler(Name).

acknowledge(Config) ->
    Name = erl_mesos_scheduler_acknowledge,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{id = OfferId, agent_id = AgentId} = Offer,
    TaskId = timestamp_task_id(),
    SchedulerPid ! {accept, OfferId, AgentId, TaskId},
    {accept, ok} = recv_reply(accept),
    {status_update, {SchedulerPid, _, _}} = recv_reply(status_update),
    SchedulerPid ! {acknowledge, AgentId, TaskId, erl_mesos_utils:uuid()},
    {acknowledge, ok} = recv_reply(acknowledge),
    ok = stop_scheduler(Name).

reconcile(Config) ->
    Name = erl_mesos_scheduler_reconcile,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{id = OfferId, agent_id = AgentId} = Offer,
    TaskId = timestamp_task_id(),
    SchedulerPid ! {accept, OfferId, AgentId, TaskId},
    {accept, ok} = recv_reply(accept),
    {status_update, {SchedulerPid, _, _}} = recv_reply(status_update),
    SchedulerPid ! {reconcile, TaskId},
    {reconcile, ok} = recv_reply(reconcile),
    {status_update, {SchedulerPid, _, EventUpdate}} = recv_reply(status_update),
    #'Event.Update'{status = Status} = EventUpdate,
    #'TaskStatus'{task_id = TaskId,
                  state = 'TASK_RUNNING',
                  reason = 'REASON_RECONCILIATION',
                  agent_id = AgentId} = Status,
    ok = stop_scheduler(Name).

request(Config) ->
    Name = erl_mesos_scheduler_request,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{agent_id = AgentId} = Offer,
    Requests = [#'Request'{agent_id = AgentId}],
    SchedulerPid ! {request, Requests},
    {request, ok} = recv_reply(request),
    ok = stop_scheduler(Name).

suppress(Config) ->
    Name = erl_mesos_scheduler_suppress,
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Name, Scheduler, SchedulerOptions1, Options),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    SchedulerPid ! suppress,
    {suppress, ok} = recv_reply(suppress),
    ok = stop_scheduler(Name).

%% Internal functions.

start_mesos_cluster(Config) ->
    erl_mesos_cluster:start(Config),
    {ok, StartTimeout} = erl_mesos_cluster:config(start_timeout, Config),
    timer:sleep(StartTimeout).

stop_mesos_cluster(Config) ->
    erl_mesos_cluster:stop(Config).

stop_mesos_master(MasterContainer, Config) ->
    erl_mesos_cluster:stop_master(MasterContainer, Config),
    {ok, LeaderElectionTimeout} =
        erl_mesos_cluster:config(leader_election_timeout, Config),
    timer:sleep(LeaderElectionTimeout).

master_container(MasterHost, Config) ->
    {ok, Masters} = erl_mesos_cluster:config(masters, Config),
    {Container, _MasterHost} = lists:keyfind(binary_to_list(MasterHost), 2,
                                             Masters),
    Container.

stop_mesos_slave(Config) ->
    erl_mesos_cluster:stop_slave(Config).

start_scheduler(Name, Scheduler, SchedulerOptions, Options) ->
    erl_mesos_scheduler:start_link(Name, Scheduler, SchedulerOptions, Options).

stop_scheduler(Name) ->
    erl_mesos_scheduler:stop(Name).

set_test_pid(SchedulerOptions) ->
    [{test_pid, self()} | SchedulerOptions].

response_pid() ->
    [{ClientRef, _Request} | _] = ets:tab2list(hackney_manager),
    {ok, Pid} = hackney_manager:async_response_pid(ClientRef),
    Pid.

recv_reply(Reply) ->
    erl_mesos_test_utils:recv_reply(Reply).

timestamp_task_id() ->
    erl_mesos_test_utils:timestamp_task_id().
