-module(erl_mesos_scheduler_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1]).

-export([bad_options/1]).

all() ->
    [bad_options].

groups() ->
    [].

init_per_suite(Config) ->
    ok = erl_mesos:start(),
    [{scheduler, erl_mesos_test_scheduler},
     {scheduler_options, []} |
    Config].

end_per_suite(_Config) ->
    application:stop(erl_mesos),
    ok.

%% Test functions.

bad_options(Config) ->
    Scheduler = ?config(scheduler, Config),
    SchedulerOptions = ?config(scheduler_options, Config),
    %% Bad options.
    Options = undefined,
    {error, {bad_options, Options}} =
        erl_mesos:start_scheduler(Scheduler, SchedulerOptions, Options),
    %% Bad master host.
    MasterHost = undefined,
    Options1 = [{master_host, MasterHost}],
    {error, {bad_master_host, MasterHost}} =
        erl_mesos:start_scheduler(Scheduler, SchedulerOptions, Options1),
    %% Bad subscribe requset options.
    SubscribeReqOptions = undefined,
    Options2 = [{subscribe_req_options, SubscribeReqOptions}],
    {error, {bad_subscribe_req_options, SubscribeReqOptions}} =
        erl_mesos:start_scheduler(Scheduler, SchedulerOptions, Options2),
    %% Bad heartbeat timeout window.
    HeartbeatTimeoutWindow = undefined,
    Options3 = [{heartbeat_timeout_window, HeartbeatTimeoutWindow}],
    {error, {bad_heartbeat_timeout_window, HeartbeatTimeoutWindow}} =
        erl_mesos:start_scheduler(Scheduler, SchedulerOptions, Options3),
    %% Bad maximu number of resubscribe.
    MaxNumResubscribe = undefined,
    Options4 = [{max_num_resubscribe, MaxNumResubscribe}],
    {error, {bad_max_num_resubscribe, MaxNumResubscribe}} =
        erl_mesos:start_scheduler(Scheduler, SchedulerOptions, Options4),
    %% Bad resubscribe timeout.
    ResubscribeTimeout = undefined,
    Options5 = [{resubscribe_timeout, ResubscribeTimeout}],
    {error, {bad_resubscribe_timeout, ResubscribeTimeout}} =
        erl_mesos:start_scheduler(Scheduler, SchedulerOptions, Options5).


