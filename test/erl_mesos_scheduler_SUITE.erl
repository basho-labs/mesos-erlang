-module(erl_mesos_scheduler_SUITE).

-include_lib("common_test/include/ct.hrl").

-export([all/0,
         init_per_suite/1,
         end_per_suite/1]).

-export([bad_options/1]).

all() ->
    [bad_options].

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
    %% Bad master host.
    MasterHost = undefined,
    Options = [{master_host, MasterHost}],
    {error, {bad_master_host, MasterHost}} =
        erl_mesos:start_scheduler(Scheduler, SchedulerOptions, Options),
    %% Bad subscribe requset options.
    SubscribeReqOptions = undefined,
    Options1 = [{subscribe_req_options, SubscribeReqOptions}],
    {error, {bad_subscribe_req_options, SubscribeReqOptions}} =
        erl_mesos:start_scheduler(Scheduler, SchedulerOptions, Options1),
    ok.
