-module(erl_mesos).

-behaviour(application).

-export([start/0]).

-export([start_scheduler/4, start_scheduler/5, stop_scheduler/1]).

-export([start/2, stop/1]).

%% @doc Starts app with deps.
-spec start() -> ok.
start() ->
    ok = application:ensure_started(asn1),
    ok = application:ensure_started(crypto),
    ok = application:ensure_started(public_key),
    ok = application:ensure_started(ssl),
    ok = application:ensure_started(idna),
    ok = application:ensure_started(hackney),
    ok = application:ensure_started(erl_mesos).

%% @equiv erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options,
%%                                  infinity)
-spec start_scheduler(term(), module(), term(),
                      erl_mesos_scheduler:options()) ->
    {ok, pid()} | {error, term()}.
start_scheduler(Ref, Scheduler, SchedulerOptions, Options) ->
    start_scheduler(Ref, Scheduler, SchedulerOptions, Options, infinity).

%% @doc Starts scheduler.
-spec start_scheduler(term(), module(), term(),
                      erl_mesos_scheduler:options(), timeout()) ->
    {ok, pid()} | {error, term()}.
start_scheduler(Ref, Scheduler, SchedulerOptions, Options, Timeout) ->
    erl_mesos_scheduler_manager:start_scheduler(Ref, Scheduler,
                                                SchedulerOptions, Options,
                                                Timeout).

%% @doc Stops scheduler.
-spec stop_scheduler(term())  -> ok | {error, term()}.
stop_scheduler(Ref) ->
    erl_mesos_scheduler_manager:stop_scheduler(Ref).

%% application callback functions.

%% @doc Starts the `erl_mesos_sup' process.
%% @private
-spec start(normal | {takeover, node()} | {failover, node()}, term()) ->
    {ok, pid()} | {error, term()}.
start(_Type, _Args) ->
    erl_mesos_sup:start_link().

%% @private
-spec stop(term()) -> ok.
stop(_State) ->
    ok.
