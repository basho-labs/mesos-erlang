-module(erl_mesos_test_scheduler).

-behaviour(erl_mesos_scheduler).

-include_lib("erl_mesos.hrl").

-export([init/1, registered/2, error/2]).

init(Options) ->

    FrameworkInfo = #framework_info{user = <<"dima 123">>,
                                    name = <<"test framework 123">>,
                                    failover_timeout = 100000.0},
    io:format("Init callback. Options: ~p~n", [Options]),
    {ok, FrameworkInfo, true, init_state}.

registered(#subscribed_packet{} = SubscribedPacket, State) ->
    io:format("Registered callback. Subscribed packet: ~p, state: ~p~n",
              [SubscribedPacket, State]),
    {ok, registered_state}.

error(#error_packet{} = ErrorPacket, State) ->
    io:format("Error callback. Error packet: ~p, state: ~p~n",
              [ErrorPacket, State]),
    {ok, error_state}.

%% framework_info(Options) ->
%%     User = proplists:get_value(user, Options, <<"">>),
%%     Name = proplists:get_value(name, Options, <<"">>),
%%     FailoverTimeout = proplists:get_value(failover_timeout, Options, 0.0),
%%     #framework_info{user = User,
%%                     name = Name,
%%                     failover_timeout = FailoverTimeout}.
