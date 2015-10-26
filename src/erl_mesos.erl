-module(erl_mesos).

-behaviour(application).

-export([start/0]).

-export([start_scheduler/3, stop_scheduler/1]).

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

%% @equiv erl_mesos_sup:start_scheduler(Scheduler, SchedulerOptions, Options).
-spec start_scheduler(module(), term(), erl_mesos_scheduler:options()) ->
    {ok, pid()} | {error, term()}.
start_scheduler(Scheduler, SchedulerOptions, Options) ->
    erl_mesos_sup:start_scheduler(Scheduler, SchedulerOptions, Options).

%% @equiv erl_mesos_sup:stop_scheduler(Pid).
-spec stop_scheduler(pid())  -> ok | {error, atom()}.
stop_scheduler(Pid) ->
    erl_mesos_sup:stop_scheduler(Pid).

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
