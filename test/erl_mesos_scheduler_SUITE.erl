-module(erl_mesos_scheduler_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0,
         init_per_suite/1,
         end_per_suite/1]).

-export([bad_options/1,
         subscribe/1]).

all() ->
    [bad_options, subscribe].

init_per_suite(Config) ->
    %% Start erl_mesos application.
    ok = erl_mesos:start(),
    %% Base config for scheduler.
    Scheduler = erl_mesos_test_scheduler,
    SchedulerOptions = [{user, <<"user">>},
                        {name, <<"erl_mesos_test_scheduler">>}],
    MasterHosts = master_hosts(),
    Options = [{master_hosts, MasterHosts}],
    [{scheduler, Scheduler},
     {scheduler_options, SchedulerOptions},
     {options, Options} |
     Config].

end_per_suite(_Config) ->
    application:stop(erl_mesos),
    ok.

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

subscribe(Config) ->
    log(?config(options, Config)),
    ok.

%% Internal functions.



master_hosts() ->
    MasterHosts = os:getenv("ERL_MESOS_TEST_MASTER_HOSTS"),
    master_hosts(MasterHosts, [], []).

master_hosts([$  | Chars], MasterHost, MasterHosts) ->
    master_hosts(Chars, MasterHost, MasterHosts);
master_hosts([$, | Chars], MasterHost, MasterHosts) ->
    master_hosts(Chars, [], [lists:reverse(MasterHost) | MasterHosts]);
master_hosts([Char | Chars], MasterHost, MasterHosts) ->
    master_hosts(Chars, [Char | MasterHost], MasterHosts);
master_hosts([], MasterHost, MasterHosts) ->
    lists:reverse([lists:reverse(MasterHost) | MasterHosts]).

log(Data) ->
    {ok, Dir} = file:get_cwd(),
    TestDir = filename:dirname(Dir),
    LogFileName = filename:join(TestDir, "log.txt"),
    file:write_file(LogFileName, io_lib:fwrite("~p.\n", [Data]), [append]).
