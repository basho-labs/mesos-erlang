-module(tmp_scheduler).

-behaviour(erl_mesos_scheduler).

-include_lib("erl_mesos.hrl").

-export([init/1,
         registered/3,
         reregistered/2,
         disconnected/2,
         resource_offers/3,
         offer_rescinded/3,
         update_status/3,
         error/3,
         handle_info/3,
         terminate/3]).

init(Options) ->
    FrameworkInfo = #framework_info{user = <<"dima">>,
                                    name = <<"Erlang test framework">>},
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

resource_offers(SchedulerInfo, #event_offers{offers = Offers}, State) ->
    io:format("Offers ~p~n", [Offers]),
    [#offer{id = #offer_id{value = OfferIdValue},
            agent_id = #agent_id{value = AgentIdValue}} | _] = Offers,

    OfferIdObj = erl_mesos_obj:new([{<<"value">>, OfferIdValue}]),

    AgentIdObj = erl_mesos_obj:new([{<<"value">>, AgentIdValue}]),

    TaskIdObj = erl_mesos_obj:new([{<<"value">>, <<"1">>}]),

%%    CommandInfoUriObj = erl_mesos_obj:new([{<<"value">>, <<"test-executor">>}]),
%%    CommandInfoObj = erl_mesos_obj:new([{<<"uris">>, [CommandInfoUriObj]},
%%                                        {<<"shall">>, false}]),

    CommandValue = <<"while true; do echo 'Test task is running...'; sleep 1; done">>,
    CommandInfoObj = erl_mesos_obj:new([{<<"shall">>, true},
                                        {<<"value">>, CommandValue}]),

    CpuScalarObj = erl_mesos_obj:new([{<<"value">>, 0.1}]),

    ResourceCpuObj = erl_mesos_obj:new([{<<"name">>, <<"cpus">>},
                                        {<<"type">>, <<"SCALAR">>},
                                        {<<"scalar">>, CpuScalarObj}]),

    TaskInfoObj = erl_mesos_obj:new([{<<"name">>, <<"TEST TASK">>},
                                     {<<"task_id">>, TaskIdObj},
                                     {<<"agent_id">>, AgentIdObj},
                                     {<<"command">>, CommandInfoObj},
                                     {<<"resources">>, [ResourceCpuObj]}]),

    LaunchObj = erl_mesos_obj:new([{<<"task_infos">>, [TaskInfoObj]}]),

    OfferOperationObj = erl_mesos_obj:new([{<<"type">>, <<"LAUNCH">>},
                                           {<<"launch">>, LaunchObj}]),

    CallAccept = #call_accept{offer_ids = [OfferIdObj],
                              operations = [OfferOperationObj]},
    Result = erl_mesos_scheduler:accept(SchedulerInfo, CallAccept),
    io:format("~n~nResult ~p~n~n", [Result]),
    erlang:send_after(5000, self(), stop),
    {ok, State}.

update_status(SchedulerInfo, #event_update{} = EventUpdate, State) ->
    call_log("== Update status callback~n"
             "== Scheduler info: ~p~n"
             "== Event update: ~p~n"
             "== State: ~p~n~n",
             [SchedulerInfo, EventUpdate, State]),
    {ok, State}.

offer_rescinded(SchedulerInfo, #event_rescind{} = EventRescind, State) ->
    call_log("== Offer rescinded callback~n"
             "== Scheduler info: ~p~n"
             "== Event rescind: ~p~n"
             "== State: ~p~n~n",
             [SchedulerInfo, EventRescind, State]),
    {ok, State}.

error(SchedulerInfo, #event_error{} = EventError, State) ->
    call_log("== Error callback~n"
             "== Scheduler info: ~p~n"
             "== Event error: ~p~n"
             "== State: ~p~n~n",
             [SchedulerInfo, EventError, State]),
    {stop, State}.

handle_info(_SchedulerInfo, stop, State) ->
    {stop, State};
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
