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

-include_lib("scheduler_info.hrl").

-include_lib("scheduler_protobuf.hrl").

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
         slave_lost/1,
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

-record(state, {user, test_pid}).

-define(LOG, false).

-define(RECV_REPLY_TIMEOUT, 10000).

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
                      slave_lost,
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
    ok = erl_mesos:start(),
    Scheduler = erl_mesos_test_scheduler,
    SchedulerOptions = [{user, "root"},
                        {name, "erl_mesos_test_scheduler"}],
    {ok, Masters} = erl_mesos_cluster:config(masters, Config),
    MasterHosts = proplists:get_keys(Masters),
    Options = [{master_hosts, MasterHosts}],
    [{log, ?LOG},
     {scheduler, Scheduler},
     {scheduler_options, SchedulerOptions},
     {options, Options} |
     Config].

end_per_suite(_Config) ->
    application:stop(erl_mesos),
    ok.

init_per_group(mesos_cluster, Config) ->
    Config.

end_per_group(mesos_cluster, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
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

%% Callbacks.

bad_options(Config) ->
    log("Bad options test cases.", Config),
    Ref = {erl_mesos_scheduler, bad_options},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    %% Bad options.
    Options = undefined,
    {error, {bad_options, Options}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options, Config),
    %% Bad master hosts.
    MasterHosts = undefined,
    Options1 = [{master_hosts, MasterHosts}],
    {error, {bad_master_hosts, MasterHosts}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options1, Config),
    MasterHosts1 = [],
    Options2 = [{master_hosts, MasterHosts1}],
    {error, {bad_master_hosts, MasterHosts1}} =
        erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options2),
    %% Bad request options.
    RequestOptions = undefined,
    Options3 = [{request_options, RequestOptions}],
    {error, {bad_request_options, RequestOptions}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options3, Config),
    %% Bad heartbeat timeout window.
    HeartbeatTimeoutWindow = undefined,
    Options4 = [{heartbeat_timeout_window, HeartbeatTimeoutWindow}],
    {error, {bad_heartbeat_timeout_window, HeartbeatTimeoutWindow}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options4, Config),
    %% Bad maximum number of resubscribe.
    MaxNumResubscribe = undefined,
    Options5 = [{max_num_resubscribe, MaxNumResubscribe}],
    {error, {bad_max_num_resubscribe, MaxNumResubscribe}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options5, Config),
    %% Bad resubscribe interval.
    ResubscribeInterval = undefined,
    Options6 = [{resubscribe_interval, ResubscribeInterval}],
    {error, {bad_resubscribe_interval, ResubscribeInterval}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options6, Config).

registered(Config) ->
    log("Registered test cases.", Config),
    Ref = {erl_mesos_scheduler, registered},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
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
    ok = stop_scheduler(Ref, Config).

disconnected(Config) ->
    log("Disconnected test cases.", Config),
    Ref = {erl_mesos_scheduler, disconnected},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    Options1 = [{max_num_resubscribe, 0} | Options],
    %% Test connection crash.
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options1,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    FormatState = format_state(SchedulerPid),
    ClientRef = state_client_ref(FormatState),
    Pid = response_pid(ClientRef),
    exit(Pid, kill),
    {disconnected, {SchedulerPid, SchedulerInfo}} = recv_reply(disconnected),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost,
                    subscribed = false,
                    stream_id = undefined} = SchedulerInfo,
    MasterHosts = proplists:get_value(master_hosts, Options),
    true = lists:member(binary_to_list(MasterHost), MasterHosts),
    %% Test cluster stop.
    Ref1 = {erl_mesos_scheduler, disconnected, 1},
    {ok, _} = start_scheduler(Ref1, Scheduler, SchedulerOptions1, Options1,
                              Config),
    {registered, {SchedulerPid1, _, _}} = recv_reply(registered),
    erl_mesos_cluster:stop(Config),
    {disconnected, {SchedulerPid1, SchedulerInfo1}} = recv_reply(disconnected),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost1,
                    subscribed = false,
                    stream_id = undefined} = SchedulerInfo1,
    true = lists:member(binary_to_list(MasterHost1), MasterHosts).

reregistered(Config) ->
    log("Reregistered test cases.", Config),
    Ref = {erl_mesos_scheduler, reregistered},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = [{failover_timeout, 1000} |
                         set_test_pid(SchedulerOptions)],
    Options = ?config(options, Config),
    Options1 = [{max_num_resubscribe, 2},
                {resubscribe_interval, 3000} |
                Options],
    %% Test connection crash.
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options1,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    FormatState = format_state(SchedulerPid),
    ClientRef = state_client_ref(FormatState),
    Pid = response_pid(ClientRef),
    exit(Pid, kill),
    {disconnected, {SchedulerPid, _}} = recv_reply(disconnected),
    {reregistered, {SchedulerPid, SchedulerInfo}} = recv_reply(reregistered),
    %% Test scheduler info.
    #scheduler_info{master_host = MasterHost,
                    subscribed = true} = SchedulerInfo,
    MasterHosts = proplists:get_value(master_hosts, Options),
    true = lists:member(binary_to_list(MasterHost), MasterHosts),
    ok = stop_scheduler(Ref, Config),
    Ref1 = {erl_mesos_scheduler, reregistered, 1},
    %% Test stop master.
    {ok, _} = start_scheduler(Ref1, Scheduler, SchedulerOptions1, Options1,
                              Config),
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
    ok = stop_scheduler(Ref1, Config).

resource_offers(Config) ->
    log("Resource offers test cases.", Config),
    Ref = {erl_mesos_scheduler, resource_offers},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    start_mesos_slave(Config),
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
    ResourceFun = fun(#'Resource'{name = Name,
                                  type = Type,
                                  scalar = Scalar,
                                  ranges = Ranges}) ->
                        true = is_list(Name),
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
    ok = stop_scheduler(Ref, Config).

offer_rescinded(Config) ->
    log("Offer rescinded test cases.", Config),
    Ref = {erl_mesos_scheduler, offer_rescinded},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    start_mesos_slave(Config),
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
    ok = stop_scheduler(Ref, Config).

status_update(Config) ->
    log("Status update test cases.", Config),
    Ref = {erl_mesos_scheduler, status_update},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    start_mesos_slave(Config),
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
    #'Event.Update'{status = Status} = EventUpdate,
    #'TaskStatus'{task_id = TaskId,
                  state = 'TASK_RUNNING',
                  source = 'SOURCE_EXECUTOR',
                  agent_id = AgentId,
                  executor_id = ExecutorId,
                  timestamp = Timestamp,
                  uuid = Uuid} = Status,
    #'ExecutorID'{value = ExecutorIdValue} = ExecutorId,
    true = is_list(ExecutorIdValue),
    true = is_float(Timestamp),
    true = is_binary(Uuid),
    ok = stop_scheduler(Ref, Config).

framework_message(Config) ->
    log("Framework message test cases.", Config),
    Ref = {erl_mesos_scheduler, framework_message},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    start_mesos_slave(Config),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{id = OfferId, agent_id = AgentId} = Offer,
    TaskId = timestamp_task_id(),
    SchedulerPid ! {accept_test_executor, OfferId, AgentId, TaskId},
    {accept, ok} = recv_reply(accept),
    {status_update, {SchedulerPid, _SchedulerInfo, _EventUpdate}} =
        recv_reply(status_update),
    ExecutorId = #'ExecutorID'{value = TaskId#'TaskID'.value},
    TestMessage = <<"test_message">>,
    Data = base64:encode(TestMessage),
    SchedulerPid ! {message, AgentId, ExecutorId, Data},
    {message, ok} = recv_reply(message),
    {framework_message, {SchedulerPid, SchedulerInfo, EventMessage}} =
        recv_reply(framework_message),
    %% Test scheduler info.
    #scheduler_info{subscribed = true} = SchedulerInfo,
    %% Test event message.
    #'Event.Message'{agent_id = AgentId,
                     executor_id = ExecutorId,
                     data = Data} = EventMessage,
    ok = stop_scheduler(Ref, Config).

slave_lost(Config) ->
    log("Slave lost test cases.", Config),
    Ref = {erl_mesos_scheduler, slave_lost},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    start_mesos_slave(Config),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{id = OfferId, agent_id = AgentId} = Offer,
    TaskId = timestamp_task_id(),
    SchedulerPid ! {accept, OfferId, AgentId, TaskId},
    {accept, ok} = recv_reply(accept),
    stop_mesos_slave(Config),
    start_mesos_slave(Config),
    {slave_lost, {SchedulerPid, SchedulerInfo, EventFailure}} =
        recv_reply(slave_lost),
    %% Test scheduler info.
    #scheduler_info{subscribed = true} = SchedulerInfo,
    %% Test event failure.
    #'Event.Failure'{agent_id = AgentId,
                     executor_id = undefined} = EventFailure,
    ok = stop_scheduler(Ref, Config).

error(Config) ->
    log("Error test cases test cases.", Config),
    Ref = {erl_mesos_scheduler, error},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = [{failover_timeout, 1} |
                         set_test_pid(SchedulerOptions)],
    Options = ?config(options, Config),
    Options1 = [{max_num_resubscribe, 1},
                {resubscribe_interval, 1500} |
                Options],
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options1,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    FormatState = format_state(SchedulerPid),
    ClientRef = state_client_ref(FormatState),
    Pid = response_pid(ClientRef),
    exit(Pid, kill),
    {disconnected, {SchedulerPid, _}} = recv_reply(disconnected),
    {error, {SchedulerPid, _SchedulerInfo, EventError}} = recv_reply(error),
    %% Test error event.
    #'Event.Error'{message = Message} = EventError,
    true = is_list(Message),
    {terminate, {SchedulerPid, _, _, _}} = recv_reply(terminate).

%% Calls.

teardown(Config) ->
    log("Teardown test cases.", Config),
    Ref = {erl_mesos_scheduler, teardown},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    SchedulerPid ! teardown,
    {teardown, ok} = recv_reply(teardown),
    {terminate, {SchedulerPid, _, _, _}} = recv_reply(terminate).

accept(Config) ->
    log("Accept test cases.", Config),
    Ref = {erl_mesos_scheduler, accept},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    start_mesos_slave(Config),
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
    ok = stop_scheduler(Ref, Config).

decline(Config) ->
    log("Decline test cases.", Config),
    Ref = {erl_mesos_scheduler, decline},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    start_mesos_slave(Config),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{id = OfferId} = Offer,
    SchedulerPid ! {decline, OfferId},
    {decline, ok} = recv_reply(decline),
    ok = stop_scheduler(Ref, Config).

revive(Config) ->
    log("Revive test cases.", Config),
    Ref = {erl_mesos_scheduler, revive},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    start_mesos_slave(Config),
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
    ok = stop_scheduler(Ref, Config).

kill(Config) ->
    log("Kill test cases.", Config),
    Ref = {erl_mesos_scheduler, kill},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    start_mesos_slave(Config),
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
    ok = stop_scheduler(Ref, Config).

shutdown(Config) ->
    log("Shutdown test cases.", Config),
    Ref = {erl_mesos_scheduler, shutdown},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    start_mesos_slave(Config),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{id = OfferId, agent_id = AgentId} = Offer,
    TaskId = timestamp_task_id(),
    SchedulerPid ! {accept, OfferId, AgentId, TaskId},
    {accept, ok} = recv_reply(accept),
    {status_update, {SchedulerPid, _, EventUpdate}} = recv_reply(status_update),
    #'Event.Update'{status = Status} = EventUpdate,
    #'TaskStatus'{executor_id = ExecutorId} = Status,
    SchedulerPid ! {shutdown, ExecutorId, AgentId},
    {shutdown, ok} = recv_reply(shutdown),
    ok = stop_scheduler(Ref, Config).

acknowledge(Config) ->
    log("Acknowledge test cases.", Config),
    Ref = {erl_mesos_scheduler, acknowledge},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    start_mesos_slave(Config),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{id = OfferId, agent_id = AgentId} = Offer,
    TaskId = timestamp_task_id(),
    SchedulerPid ! {accept, OfferId, AgentId, TaskId},
    {accept, ok} = recv_reply(accept),
    {status_update, {SchedulerPid, _, _}} = recv_reply(status_update),
    SchedulerPid ! {acknowledge, AgentId, TaskId, <<"1">>},
    {acknowledge, ok} = recv_reply(acknowledge),
    ok = stop_scheduler(Ref, Config).

reconcile(Config) ->
    log("Reconcile test cases.", Config),
    Ref = {erl_mesos_scheduler, reconcile},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    start_mesos_slave(Config),
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
    ok = stop_scheduler(Ref, Config).

request(Config) ->
    log("Request test cases.", Config),
    Ref = {erl_mesos_scheduler, request},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    start_mesos_slave(Config),
    {resource_offers, {SchedulerPid, _, EventOffers}} =
        recv_reply(resource_offers),
    #'Event.Offers'{offers = [Offer | _]} = EventOffers,
    #'Offer'{agent_id = AgentId} = Offer,
    Requests = [#'mesos_v1_Request'{agent_id = AgentId}],
    SchedulerPid ! {request, Requests},
    {request, ok} = recv_reply(request),
    ok = stop_scheduler(Ref, Config).

suppress(Config) ->
    log("Suppress test cases.", Config),
    Ref = {erl_mesos_scheduler, suppress},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options,
                              Config),
    {registered, {SchedulerPid, _, _}} = recv_reply(registered),
    SchedulerPid ! suppress,
    {suppress, ok} = recv_reply(suppress),
    ok = stop_scheduler(Ref, Config).

%% Internal functions.

start_mesos_cluster(Config) ->
    log("Start test mesos cluster.", Config),
    erl_mesos_cluster:start(Config),
    {ok, StartTimeout} = erl_mesos_cluster:config(start_timeout, Config),
    timer:sleep(StartTimeout).

stop_mesos_cluster(Config) ->
    log("Stop test mesos cluster.", Config),
    erl_mesos_cluster:stop(Config).

stop_mesos_master(MasterContainer, Config) ->
    log("Stop test mesos master. Master container: ~s.", [MasterContainer],
        Config),
    erl_mesos_cluster:stop_master(MasterContainer, Config),
    {ok, LeaderElectionTimeout} =
        erl_mesos_cluster:config(leader_election_timeout, Config),
    timer:sleep(LeaderElectionTimeout).

master_container(MasterHost, Config) ->
    {ok, Masters} = erl_mesos_cluster:config(masters, Config),
    proplists:get_value(binary_to_list(MasterHost), Masters).

start_mesos_slave(Config) ->
    log("Start test mesos slave.", Config),
    erl_mesos_cluster:start_slave(Config),
    {ok, SlaveStartTimeout} = erl_mesos_cluster:config(slave_start_timeout,
                                                       Config),
    timer:sleep(SlaveStartTimeout).

stop_mesos_slave(Config) ->
    log("Stop test mesos slave.", Config),
    erl_mesos_cluster:stop_slave(Config).

start_scheduler(Ref, Scheduler, SchedulerOptions, Options, Config) ->
    log("Start scheduler. Ref: ~p, Scheduler: ~p.", [Ref, Scheduler], Config),
    erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options).

stop_scheduler(Ref, Config) ->
    log("Stop scheduler. Ref: ~p.", [Ref], Config),
    erl_mesos:stop_scheduler(Ref).

set_test_pid(SchedulerOptions) ->
    [{test_pid, self()} | SchedulerOptions].

format_state(SchedulerPid) ->
    {status, _Pid, _Module, Items} = sys:get_status(SchedulerPid),
    {data, Format} = lists:last(lists:last(Items)),
    proplists:get_value("State", Format).

state_client_ref(FormatState) ->
    State = proplists:get_value("State", FormatState),
    proplists:get_value(client_ref, State).

response_pid(ClientRef) ->
    {ok, Pid} = hackney_manager:async_response_pid(ClientRef),
    Pid.

recv_reply(Reply) ->
    receive
        {Reply, Data} ->
            {Reply, Data}
    after ?RECV_REPLY_TIMEOUT ->
        {error, timeout}
    end.

timestamp_task_id() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    Timestamp = (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs,
    #'TaskID'{value = integer_to_list(Timestamp)}.

log(Format, Config) ->
    log(Format, [], Config).

log(Format, Data, Config) ->
    case ?config(log, Config) of
        true ->
            ct:pal(Format, Data);
        false ->
            ok
    end.
