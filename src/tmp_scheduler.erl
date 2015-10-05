-module(tmp_scheduler).

-behaviour(erl_mesos_scheduler).

-include_lib("erl_mesos.hrl").

-export([init/1, registered/2]).

init(_Options) ->
    FrameworkInfo = #framework_info{user = <<"dima 123">>,
                                    name = <<"test framework 123">>,
                                    failover_timeout = 100000.0},
    {ok, FrameworkInfo, true, init_state}.

registered(#subscribed{} = Subscribed, State) ->
    io:format("Registered callback ~p~n", [[Subscribed, State]]),
    {ok, registered_state}.



