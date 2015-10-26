-module(erl_mesos).

-behaviour(application).

-export([start/0]).

-export([start/2, stop/1]).

%% application callback functions.

start() ->
    ok = application:start(asn1),
    ok = application:start(crypto),
    ok = application:start(public_key),
    ok = application:start(ssl),
    ok = application:start(idna),
    ok = application:start(hackney),
    ok = application:start(erl_mesos).

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
