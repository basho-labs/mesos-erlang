-module(erl_mesos_scheduler_SUITE).

-include_lib("common_test/include/ct.hrl").

-include_lib("erl_mesos.hrl").

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
         disconnected/1]).

-record(state, {callback,
                test_pid}).

all() ->
    [bad_options, {group, cluster}].

groups() ->
    [{cluster, [registered, disconnected]}].

init_per_suite(Config) ->
    ok = erl_mesos:start(),
    Scheduler = erl_mesos_test_scheduler,
    SchedulerOptions = [{user, <<"user">>},
                        {name, <<"erl_mesos_test_scheduler">>}],
    {ok, Masters} = erl_mesos_cluster:config(masters, Config),
    MasterHosts = proplists:get_keys(Masters),
    Options = [{master_hosts, MasterHosts}],
    [{scheduler, Scheduler},
     {scheduler_options, SchedulerOptions},
     {options, Options} |
     Config].

end_per_suite(_Config) ->
    application:stop(erl_mesos),
    ok.

init_per_group(cluster, Config) ->
    Config.

end_per_group(cluster, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    case lists:member(TestCase, proplists:get_value(cluster, groups())) of
        true ->
            erl_mesos_cluster:stop(Config),
            StartRes = erl_mesos_cluster:start(Config),
            ct:pal("Start test mesos cluster.~n"
                   "Output: ~s~n", [StartRes]),
            {ok, StartTimeout} = erl_mesos_cluster:config(start_timeout,
                                                          Config),
            timer:sleep(StartTimeout),
            Config;
        false ->
            Config
    end.

end_per_testcase(TestCase, Config) ->
    case lists:member(TestCase, proplists:get_value(cluster, groups())) of
        true ->
            StopRes = erl_mesos_cluster:stop(Config),
            ct:pal("Stop test mesos cluster.~n"
                   "Output: ~s~n", [StopRes]),
            Config;
        false ->
            Config
    end.

%% Test functions.

bad_options(Config) ->
    ct:pal("** Bad options test cases"),
    Ref = {erl_mesos_scheduler, bad_options},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    %% Bad options.
    Options = undefined,
    {error, {bad_options, Options}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options),
    %% Bad master hosts.
    MasterHosts = undefined,
    Options1 = [{master_hosts, MasterHosts}],
    {error, {bad_master_hosts, MasterHosts}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options1),
    MasterHosts1 = [],
    Options2 = [{master_hosts, MasterHosts1}],
    {error, {bad_master_hosts, MasterHosts1}} =
        erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options2),
    %% Bad subscribe requset options.
    SubscribeReqOptions = undefined,
    Options3 = [{subscribe_req_options, SubscribeReqOptions}],
    {error, {bad_subscribe_req_options, SubscribeReqOptions}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options3),
    %% Bad heartbeat timeout window.
    HeartbeatTimeoutWindow = undefined,
    Options4 = [{heartbeat_timeout_window, HeartbeatTimeoutWindow}],
    {error, {bad_heartbeat_timeout_window, HeartbeatTimeoutWindow}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options4),
    %% Bad maximum number of resubscribe.
    MaxNumResubscribe = undefined,
    Options5 = [{max_num_resubscribe, MaxNumResubscribe}],
    {error, {bad_max_num_resubscribe, MaxNumResubscribe}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options5),
    %% Bad resubscribe interval.
    ResubscribeInterval = undefined,
    Options6 = [{resubscribe_interval, ResubscribeInterval}],
    {error, {bad_resubscribe_interval, ResubscribeInterval}} =
        start_scheduler(Ref, Scheduler, SchedulerOptions, Options6).

registered(Config) ->
    ct:pal("** Registered test cases"),
    Ref = {erl_mesos_scheduler, registered},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options),
    {registered, SchedulerPid, SchedulerInfo, SubscribedEvent} = recv_reply(),
    %% Test scheduler info.
    MasterHost = erl_mesos_scheduler:master_host(SchedulerInfo),
    MasterHosts = proplists:get_value(master_hosts, Options),
    true = lists:member(binary_to_list(MasterHost), MasterHosts),
    true = erl_mesos_scheduler:subscribed(SchedulerInfo),
    FrameworkId = erl_mesos_scheduler:framework_id(SchedulerInfo),
    %% Test subscribed event.
    #subscribed_event{framework_id = FrameworkId,
                      heartbeat_interval_seconds = HeartbeatIntervalSeconds} =
        SubscribedEvent,
    true = is_integer(HeartbeatIntervalSeconds),
    %% Test scheduler state.
    FormatState = format_state(SchedulerPid),
    #state{callback = registered} = scheduler_state(FormatState),
    ok = stop_scheduler(Ref).

disconnected(Config) ->
    ct:pal("** Disconnected test cases"),
    Ref = {erl_mesos_scheduler, disconnected},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    Options1 = [{max_num_resubscribe, 0} | Options],
    %% Test connection crash.
    {ok, _} = start_scheduler(Ref, Scheduler, SchedulerOptions1, Options1),
    {registered, SchedulerPid, _, _} = recv_reply(),
    FormatState = format_state(SchedulerPid),
    ClientRef = state_client_ref(FormatState),
    Pid = response_pid(ClientRef),
    exit(Pid, kill),
    {disconnected, SchedulerPid, SchedulerInfo} = recv_reply(),
    %% Test scheduler info.
    MasterHost = erl_mesos_scheduler:master_host(SchedulerInfo),
    MasterHosts = proplists:get_value(master_hosts, Options),
    true = lists:member(binary_to_list(MasterHost), MasterHosts),
    false = erl_mesos_scheduler:subscribed(SchedulerInfo),
    %% Test scheduler state.
    stop_scheduler(Ref),
    {terminate, SchedulerPid, _, _, State} = recv_reply(),
    #state{callback = disconnected} = State,
    %% Test claster stop.
    Ref1 = {erl_mesos_scheduler, disconnected, 1},
    {ok, _} = start_scheduler(Ref1, Scheduler, SchedulerOptions1, Options1),
    {registered, SchedulerPid1, _, _} = recv_reply(),
    erl_mesos_cluster:stop(Config),
    {disconnected, SchedulerPid1, SchedulerInfo1} = recv_reply(),
    %% Test scheduler info.
    MasterHost1 = erl_mesos_scheduler:master_host(SchedulerInfo1),
    true = lists:member(binary_to_list(MasterHost1), MasterHosts),
    false = erl_mesos_scheduler:subscribed(SchedulerInfo1),
    %% Test scheduler state.
    {terminate, SchedulerPid1, _, _, State1} = recv_reply(),
    #state{callback = disconnected} = State1.

%% Internal functions.

set_test_pid(SchedulerOptions) ->
    [{test_pid, self()} | SchedulerOptions].

recv_reply() ->
    receive
        {registered, SchedulerPid, SchedulerInfo, SubscribedEvent} ->
            {registered, SchedulerPid, SchedulerInfo, SubscribedEvent};
        {disconnected, SchedulerPid, SchedulerInfo} ->
            {disconnected, SchedulerPid, SchedulerInfo};
        {terminate, SchedulerPid, SchedulerInfo, Reason, State} ->
            {terminate, SchedulerPid, SchedulerInfo, Reason, State};
        Reply ->
            {error, {bad_reply, Reply}}
    after 5000 ->
        {error, timeout}
    end.

scheduler_state(FormatState) ->
    proplists:get_value("Scheduler state", FormatState).

state_client_ref(FormatState) ->
    State = proplists:get_value("State", FormatState),
    proplists:get_value(client_ref, State).

format_state(SchedulerPid) ->
    {status, _Pid, _Module, Items} = sys:get_status(SchedulerPid),
    {data, Format} = lists:last(lists:last(Items)),
    proplists:get_value("State", Format).

start_scheduler(Ref, Scheduler, SchedulerOptions, Options) ->
    ct:pal("** Start scheduler~n"
           "** Ref == ~p~n"
           "** Scheduler == ~p~n"
           "** Scheduler options == ~p~n"
           "** Options == ~p~n",
           [Ref, Scheduler, SchedulerOptions, Options]),
    erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options).

stop_scheduler(Ref) ->
    ct:pal("** Stop scheduler~n"
           "** Ref == ~p~n",
           [Ref]),
    erl_mesos:stop_scheduler(Ref).

response_pid(ClientRef) ->
    {ok, Pid} = hackney_manager:async_response_pid(ClientRef),
    Pid.
