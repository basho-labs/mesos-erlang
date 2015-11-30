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
         registered/1]).

-record(state, {callback,
                test_pid}).

all() ->
    [bad_options, {group, cluster}].

groups() ->
    [{cluster, [registered]}].

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
            erl_mesos_cluster:start(Config),
            Config;
        false ->
            Config
    end.

end_per_testcase(TestCase, Config) ->
    case lists:member(TestCase, proplists:get_value(cluster, groups())) of
        true ->
            erl_mesos_cluster:stop(Config),
            Config;
        false ->
            Config
    end.

%% Test functions.

bad_options(Config) ->
    Ref = {erl_mesos_scheduler, bad_options},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    %% Bad options.
    Options = undefined,
    {error, {bad_options, Options}} =
        erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options),
    %% Bad master hosts.
    MasterHosts = undefined,
    Options1 = [{master_hosts, MasterHosts}],
    {error, {bad_master_hosts, MasterHosts}} =
        erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options1),
    MasterHosts1 = [],
    Options2 = [{master_hosts, MasterHosts1}],
    {error, {bad_master_hosts, MasterHosts1}} =
        erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options2),
    %% Bad subscribe requset options.
    SubscribeReqOptions = undefined,
    Options3 = [{subscribe_req_options, SubscribeReqOptions}],
    {error, {bad_subscribe_req_options, SubscribeReqOptions}} =
        erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options3),
    %% Bad heartbeat timeout window.
    HeartbeatTimeoutWindow = undefined,
    Options4 = [{heartbeat_timeout_window, HeartbeatTimeoutWindow}],
    {error, {bad_heartbeat_timeout_window, HeartbeatTimeoutWindow}} =
        erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options4),
    %% Bad maximum number of resubscribe.
    MaxNumResubscribe = undefined,
    Options5 = [{max_num_resubscribe, MaxNumResubscribe}],
    {error, {bad_max_num_resubscribe, MaxNumResubscribe}} =
        erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options5),
    %% Bad resubscribe interval.
    ResubscribeInterval = undefined,
    Options6 = [{resubscribe_interval, ResubscribeInterval}],
    {error, {bad_resubscribe_interval, ResubscribeInterval}} =
        erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options6).

registered(Config) ->
    Ref = {erl_mesos_scheduler, subscribe},
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    SchedulerOptions1 = set_test_pid(SchedulerOptions),
    Options = ?config(options, Config),
    {ok, _Pid} = erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions1,
                                           Options),
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
    #state{callback = registered} = scheduler_state(FormatState).

%% Internal functions.

set_test_pid(SchedulerOptions) ->
    [{test_pid, self()} | SchedulerOptions].

recv_reply() ->
    receive
        {registered, SchedulerPid, SchedulerInfo, SubscribedEvent} ->
            {registered, SchedulerPid, SchedulerInfo, SubscribedEvent}
    after 10000 ->
        {error, timeout}
    end.

scheduler_state(FormatState) ->
    proplists:get_value("Scheduler state", FormatState).

format_state(SchedulerPid) ->
    {status, _Pid, _Module, Items} = sys:get_status(SchedulerPid),
    {data, Format} = lists:last(lists:last(Items)),
    proplists:get_value("State", Format).

log(Data) ->
    {ok, Dir} = file:get_cwd(),
    TestDir = filename:dirname(Dir),
    LogFileName = filename:join(TestDir, "log.txt"),
    file:write_file(LogFileName, io_lib:fwrite("~p\n", [Data]), [append]).
