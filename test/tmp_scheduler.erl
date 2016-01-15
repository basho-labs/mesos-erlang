-module(tmp_scheduler).

-behaviour(erl_mesos_scheduler).

-include_lib("scheduler_info.hrl").

-include_lib("scheduler_protobuf.hrl").

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

-record(state, {offer = accept :: accept | decline}).

init(Options) ->
    FrameworkInfo = #'FrameworkInfo'{user = <<"root">>,
                                     name = <<"Erlang test framework">>},
    call_log("== Init callback~n"
             "== Options: ~p~n~n", [Options]),
    Offer = proplists:get_value(offer, Options, accept),
    {ok, FrameworkInfo, true, #state{offer = Offer}}.

registered(_SchedulerInfo, EventSubscribed, State) ->
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

resource_offers(#scheduler_info{framework_id = FrameworkId} = SchedulerInfo,
                #'Event.Offers'{offers = Offers} = EventOffers,
                State) ->
%%     TaskIdValue = timestamp_task_id_value(),
%%     State1 =
%%     case State of
%%         #state{offer = accept} ->
%%             [#offer{id = OfferId, agent_id = AgentId} | _] = Offers,
%%             ExecutorResources = [#resource{name = <<"cpus">>,
%%                                            type = <<"SCALAR">>,
%%                                            scalar = #value_scalar{value = 0.1}},
%%                                  #resource{name = <<"mem">>,
%%                                            type = <<"SCALAR">>,
%%                                            scalar = #value_scalar{value = 128.0}}],
%%             CommandInfoUris =
%%                 [#command_info_uri{value = <<"test_executor">>,
%%                                    extract = false,
%%                                    executable = true},
%%                  #command_info_uri{value = <<"test_executor.py">>,
%%                                    extract = false,
%%                                    executable = false}],
%%             ExecutorId = #executor_id{value = TaskIdValue},
%%             ExecutorInfo =
%%                 #executor_info{executor_id = ExecutorId,
%%                                framework_id = FrameworkId,
%%                                command =
%%                                    #command_info{shell = true,
%%                                                  user = <<"root">>,
%%                                                  uris = CommandInfoUris,
%%                                                  value = <<"./test_executor">>},
%%                                resources = ExecutorResources,
%%                                name = list_to_binary(binary_to_list(TaskIdValue) ++ " Executor"),
%%                                source = <<"Tmp">>},
%%             TaskResources = [#resource{name = <<"cpus">>,
%%                                        type = <<"SCALAR">>,
%%                                        scalar = #value_scalar{value = 0.1}},
%%                              #resource{name = <<"mem">>,
%%                                        type = <<"SCALAR">>,
%%                                        scalar = #value_scalar{value = 128.0}},
%%                              #resource{name = <<"disk">>,
%%                                        type = <<"SCALAR">>,
%%                                        scalar = #value_scalar{value = 512.0}}],
%%             TaskInfo = #task_info{name = <<"test_task">>,
%%                                   task_id = #task_id{value = TaskIdValue},
%%                                   agent_id = AgentId,
%%                                   executor = ExecutorInfo,
%%                                   resources = TaskResources},
%%             Launch = #offer_operation_launch{task_infos = [TaskInfo]},
%%             OfferOperation = #offer_operation{type = <<"LAUNCH">>,
%%                                               launch = Launch},
%%             ok = erl_mesos_scheduler:accept(SchedulerInfo, [OfferId],
%%                                             [OfferOperation]),
%%             Message = <<"test_message">>,
%%             erlang:send_after(1000, self(), {send_message, AgentId, ExecutorId, Message}),
%%             State#state{offer = decline};
%%         #state{offer = decline} ->
%%             State
%%     end,
    call_log("== Resource offers callback~n"
             "== Event offers: ~p~n"
             "== Offer: ~p~n",
             [EventOffers, State#state.offer]),
    {ok, State}.

offer_rescinded(_SchedulerInfo, #'Event.Rescind'{} = EventRescind, State) ->
    call_log("== Offer rescinded callback~n"
             "== Event rescind: ~p~n",
             [EventRescind]),
    {ok, State}.

status_update(_SchedulerInfo, #'Event.Update'{} = EventUpdate, State) ->
    call_log("== Update status callback~n"
             "== Event update: ~p~n",
             [EventUpdate]),
    {ok, State}.

framework_message(_SchedulerInfo, #'Event.Message'{} = EventMessage, State) ->
    call_log("== Framework message callback~n"
             "== Event message: ~p~n",
             [EventMessage]),
    {ok, State}.

slave_lost(_SchedulerInfo, #'Event.Failure'{} = EventFailure, State) ->
    call_log("== Slave lost callback~n"
             "== Event failure: ~p~n",
             [EventFailure]),
    {ok, State}.

executor_lost(_SchedulerInfo, #'Event.Failure'{} = EventFailure, State) ->
    call_log("== Executor lost callback~n"
             "== Event failure: ~p~n",
             [EventFailure]),
    {ok, State}.

error(_SchedulerInfo, #'Event.Error'{} = EventError, State) ->
    call_log("== Error callback~n"
             "== Event error: ~p~n",
             [EventError]),
    {stop, State}.

%% handle_info(SchedulerInfo, {send_message, AgentId, ExecutorId, Message},
%%             State) ->
%%     Data = base64:encode(Message),
%%     ok = erl_mesos_scheduler:message(SchedulerInfo, AgentId, ExecutorId, Data),
%%     {ok, State};
%% handle_info(_SchedulerInfo, stop, State) ->
%%     {stop, State};
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

timestamp_task_id_value() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    Timestamp = (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs,
    list_to_binary(integer_to_list(Timestamp)).
