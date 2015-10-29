-module(tmp_scheduler).

-behaviour(erl_mesos_scheduler).

-include_lib("erl_mesos.hrl").

-export([init/1,
         registered/3,
         reregistered/2,
         disconnected/2,
         error/3,
         handle_info/3,
         terminate/3]).

init(Options) ->
    FrameworkInfo = #framework_info{user = <<"dima 123">>,
                                    name = <<"test framework 123">>,
                                    failover_timeout = 100000.0},
    io:format("== Init callback~n"
              "== Options: ~p~n~n", [Options]),
    {ok, FrameworkInfo, true, init_state}.

registered(Scheduler, #subscribed_event{} = SubscribedEvent, State) ->
    io:format("== Registered callback~n"
              "== Scheduler: ~p~n"
              "== Subscribed event: ~p~n"
              "== State: ~p~n~n",
              [Scheduler, SubscribedEvent, State]),
    {ok, registered_state}.

reregistered(Scheduler, State) ->
    io:format("== Reregistered callback~n"
              "== Scheduler: ~p~n"
              "== State: ~p~n~n",
              [Scheduler, State]),
    {ok, reregistered_state}.

disconnected(Scheduler, State) ->
    io:format("== Disconnected callback~n"
              "== Scheduler: ~p~n"
              "== State: ~p~n~n",
              [Scheduler, State]),
    {ok, disconnected_state}.

error(Scheduler, #error_event{} = ErrorEvent, State) ->
    io:format("== Error callback~n"
              "== Scheduler: ~p~n"
              "== Error event: ~p~n"
              "== State: ~p~n~n",
              [Scheduler, ErrorEvent, State]),
    {ok, error_state}.

handle_info(_Scheduler, stop, State) ->
    {stop, State};
handle_info(Scheduler, teardown, _State) ->
    ok = erl_mesos_scheduler:teardown(Scheduler),
    {ok, handle_info_state};
handle_info(Scheduler, Info, State) ->
    io:format("== Info callback~n"
              "== Scheduler: ~p~n"
              "== Info: ~p~n"
              "== State: ~p~n~n",
              [Scheduler, Info, State]),
    {ok, handle_info_state}.

terminate(Scheduler, Reason, State) ->
    io:format("== Terminate callback~n"
              "== Scheduler: ~p~n"
              "== Reason: ~p~n"
              "== State: ~p~n~n",
              [Scheduler, Reason, State]).
