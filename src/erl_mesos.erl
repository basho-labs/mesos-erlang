-module(erl_mesos).

-behaviour(application).

-export([start/0]).

-export([start_scheduler/4, stop_scheduler/1]).

-export([start/2, stop/1]).

%% @doc Starts app with deps.
-spec start() -> ok.
start() ->
    ok = application:start(asn1),
    ok = application:start(crypto),
    ok = application:start(public_key),
    ok = application:start(ssl),
    ok = application:start(idna),
    ok = application:start(hackney),
    ok = application:start(erl_mesos).

%% @equiv erl_mesos_scheduler_manager:start_scheduler(Ref, Scheduler,
%%                                                    SchedulerOptions,
%%                                                    Options).
-spec start_scheduler(term(), module(), term(),
                      erl_mesos_scheduler:options()) ->
    {ok, pid()} | {error, term()}.
start_scheduler(Ref, Scheduler, SchedulerOptions, Options) ->
    erl_mesos_scheduler_manager:start_scheduler(Ref, Scheduler,
                                                SchedulerOptions, Options).

%% @equiv erl_mesos_scheduler_manager:stop_scheduler(Ref).
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
