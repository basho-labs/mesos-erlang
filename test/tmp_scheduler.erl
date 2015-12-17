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
    call_log("== Init callback~n"
             "== Options: ~p~n~n", [Options]),
    {ok, FrameworkInfo, true, init_state}.

registered(SchedulerInfo, #event_subscribed{} = EventSubscribed, State) ->
    call_log("== Registered callback~n"
             "== Scheduler info: ~p~n"
             "== Event subscribed: ~p~n"
             "== State: ~p~n~n",
             [SchedulerInfo, EventSubscribed, State]),
    {ok, registered_state}.

reregistered(SchedulerInfo, State) ->
    call_log("== Reregistered callback~n"
             "== Scheduler info: ~p~n"
             "== State: ~p~n~n",
             [SchedulerInfo, State]),
    {ok, reregistered_state}.

disconnected(SchedulerInfo, State) ->
    call_log("== Disconnected callback~n"
             "== Scheduler info: ~p~n"
             "== State: ~p~n~n",
             [SchedulerInfo, State]),
    {ok, disconnected_state}.

error(SchedulerInfo, #event_error{} = EventError, State) ->
    call_log("== Error callback~n"
             "== Scheduler info: ~p~n"
             "== Event error : ~p~n"
             "== State: ~p~n~n",
             [SchedulerInfo, EventError, State]),
    {ok, error_state}.

handle_info(_SchedulerInfo, stop, State) ->
    {stop, State};
%% handle_info(SchedulerInfo, teardown, _State) ->
%%     ok = erl_mesos_scheduler:teardown(SchedulerInfo),
%%     {ok, handle_info_state};
handle_info(SchedulerInfo, Info, State) ->
    call_log("== Info callback~n"
             "== Scheduler info: ~p~n"
             "== Info: ~p~n"
             "== State: ~p~n~n",
             [SchedulerInfo, Info, State]),
    {ok, handle_info_state}.

terminate(SchedulerInfo, Reason, State) ->
    io:format("== Terminate callback~n"
              "== Scheduler info: ~p~n"
              "== Reason: ~p~n"
              "== State: ~p~n~n",
              [SchedulerInfo, Reason, State]).

call_log(Format, Data) ->
    erl_mesos_logger:info(Format, Data).
