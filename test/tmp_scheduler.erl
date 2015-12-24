-module(tmp_scheduler).

-behaviour(erl_mesos_scheduler).

-include_lib("erl_mesos.hrl").

-export([init/1,
         registered/3,
         reregistered/2,
         disconnected/2,
         resource_offers/3,
         offer_rescinded/3,
         status_update/3,
         framework_message/3,
         slave_lost/3,
         executor_lost/3,
         error/3,
         handle_info/3,
         terminate/3]).

-record(state, {offer = accept :: accept | reconcile | decline}).

init(Options) ->
    FrameworkInfo = #framework_info{user = <<"root">>,
                                    name = <<"Erlang test framework">>},
    call_log("== Init callback~n"
             "== Options: ~p~n~n", [Options]),
    Offer = proplists:get_value(offer, Options, accept),
    {ok, FrameworkInfo, true, #state{offer = Offer}}.

registered(_SchedulerInfo, #event_subscribed{} = EventSubscribed, State) ->
    call_log("== Registered callback~n"
             "== Event subscribed: ~p~n",
             [EventSubscribed]),
    {ok, State}.

reregistered(_SchedulerInfo, State) ->
    call_log("== Reregistered callback~n", []),
    {ok, State}.

disconnected(_SchedulerInfo, State) ->
    call_log("== Disconnected callback~n", []),
    {ok, State}.

resource_offers(SchedulerInfo, #event_offers{offers = Offers} = EventOffers,
                State) ->
    TaskId = #task_id{value = <<"3">>},
    State1 =
    case State of
        #state{offer = accept} ->
            [#offer{id = OfferId, agent_id = AgentId} | _] = Offers,
            CommandValue = <<"while true; sleep 1; done">>,
            CommandInfo = #command_info{shell = true,
                                        value = CommandValue},
            CpuScalarValue = #value_scalar{value = 0.1},
            ResourceCpu = #resource{name = <<"cpus">>,
                                    type = <<"SCALAR">>,
                                    scalar = CpuScalarValue},
            TaskInfo = #task_info{name = <<"test_task">>,
                                  task_id = TaskId,
                                  agent_id = AgentId,
                                  command = CommandInfo,
                                  resources = [ResourceCpu]},
            Launch = #offer_operation_launch{task_infos = [TaskInfo]},
            OfferOperation = #offer_operation{type = <<"LAUNCH">>,
                                              launch = Launch},
            ok = erl_mesos_scheduler:accept(SchedulerInfo, [OfferId],
                                            [OfferOperation]),
            CallReconcileTask = #call_reconcile_task{task_id = TaskId},
            ok = erl_mesos_scheduler:reconcile(SchedulerInfo,
                                               [CallReconcileTask]),
            State#state{offer = decline};
        #state{offer = decline} ->
            State
    end,
    call_log("== Resource offers callback~n"
             "== Event offers: ~p~n"
             "== Offer: ~p~n",
             [EventOffers, State#state.offer]),
    {ok, State1}.

offer_rescinded(_SchedulerInfo, #event_rescind{} = EventRescind, State) ->
    call_log("== Offer rescinded callback~n"
             "== Event rescind: ~p~n",
             [EventRescind]),
    {ok, State}.

status_update(_SchedulerInfo, #event_update{} = EventUpdate, State) ->
    call_log("== Update status callback~n"
             "== Event update: ~p~n",
             [EventUpdate]),
    {ok, State}.

framework_message(_SchedulerInfo, #event_message{} = EventMessage, State) ->
    call_log("== Framework message callback~n"
             "== Event message: ~p~n",
             [EventMessage]),
    {ok, State}.

slave_lost(_SchedulerInfo, #event_failure{} = EventFailure, State) ->
    call_log("== Slave lost callback~n"
             "== Event failure: ~p~n",
             [EventFailure]),
    {ok, State}.

executor_lost(_SchedulerInfo, #event_failure{} = EventFailure, State) ->
    call_log("== Executor lost callback~n"
             "== Event failure: ~p~n",
             [EventFailure]),
    {ok, State}.

error(_SchedulerInfo, #event_error{} = EventError, State) ->
    call_log("== Error callback~n"
             "== Event error: ~p~n",
             [EventError]),
    {stop, State}.

handle_info(_SchedulerInfo, stop, State) ->

    {stop, State};
handle_info(_SchedulerInfo, Info, State) ->
    call_log("== Info callback~n"
             "== Info: ~p~n",
             [Info]),
    {ok, State}.

terminate(_SchedulerInfo, Reason, State) ->
    io:format("== Terminate callback~n"
              "== Reason: ~p~n"
              "== State: ~p~n~n",
              [Reason, State]).

call_log(Format, Data) ->
    erl_mesos_logger:info(Format, Data).
