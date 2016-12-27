%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 Basho Technologies Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License. You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(erl_mesos_smart_scheduler).

-behaviour(erl_mesos_scheduler).

-include("erl_mesos_scheduler_info.hrl").

-export([start_link/4]).

-export([teardown/1,
         accept/3,
         accept/4,
         accept_inverse_offers/2,
         accept_inverse_offers/3,
         decline/2,
         decline/3,
         decline_inverse_offers/2,
         decline_inverse_offers/3,
         revive/1,
         kill/2,
         kill/3,
         shutdown/2,
         shutdown/3,
         acknowledge/4,
         reconcile/2,
         message/4,
         request/2,
         suppress/1]).

-export([init/1,
         registered/3,
         disconnected/2,
         reregistered/2,
         resource_offers/3,
         resource_inverse_offers/3,
         offer_rescinded/3,
         inverse_offer_rescinded/3,
         status_update/3,
         framework_message/3,
         slave_lost/3,
         executor_lost/3,
         error/3,
         handle_info/3,
         terminate/2]).

-record(state, {name :: atom(),
                scheduler :: module(),
                scheduler_state :: module(),
                max_num_execute :: pos_integer(),
                max_queue_length :: pos_integer(),
                num_execute = 0 :: non_neg_integer(),
                queue = [] :: [queue_call()]}).

-type options() :: [{atom(), [term()]}].
-export_type([options/0]).

-type state() :: #state{}.

-type queue_call() :: {atom(), [term()]}.

%% Callbacks.

-callback init(term()) ->
    {ok, erl_mesos:'FrameworkInfo'(), term()} | {stop, term()}.

-callback registered(erl_mesos_scheduler:scheduler_info(),
                     erl_mesos_scheduler:'Event.Subscribed'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback disconnected(erl_mesos_scheduler:scheduler_info(), term()) ->
    {ok, term()} | {stop, term()}.

-callback reregistered(erl_mesos_scheduler:scheduler_info(), term()) ->
    {ok, term()} | {stop, term()}.

-callback resource_offers(erl_mesos_scheduler:scheduler_info(),
                          erl_mesos_scheduler:'Event.Offers'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback resource_inverse_offers(erl_mesos_scheduler:scheduler_info(),
                                  erl_mesos_scheduler:'Event.InverseOffers'(),
                                  term()) ->
    {ok, term()} | {stop, term()}.

-callback offer_rescinded(erl_mesos_scheduler:scheduler_info(),
                          erl_mesos_scheduler:'Event.Rescind'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback inverse_offer_rescinded(erl_mesos_scheduler:scheduler_info(),
                             erl_mesos_scheduler:'Event.RescindInverseOffer'(),
                             term()) ->
    {ok, term()} | {stop, term()}.

-callback status_update(erl_mesos_scheduler:scheduler_info(),
                        erl_mesos_scheduler:'Event.Update'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback framework_message(erl_mesos_scheduler:scheduler_info(),
                            erl_mesos_scheduler:'Event.Message'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback slave_lost(erl_mesos_scheduler:scheduler_info(),
                     erl_mesos_scheduler:'Event.Failure'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback executor_lost(erl_mesos_scheduler:scheduler_info(),
                        erl_mesos_scheduler:'Event.Failure'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback error(erl_mesos_scheduler:scheduler_info(),
                erl_mesos_scheduler:'Event.Error'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback handle_info(erl_mesos_scheduler:scheduler_info(), term(), term()) ->
    {ok, term()} | {stop, term()}.

-callback terminate(term(), term()) -> term().

-define(DEFAULT_MAX_NUM_EXECUTE, 5).

-define(DEFAULT_MAX_QUEUE_LENGTH, 100).

%% External functions.

%% @doc Starts the `erl_mesos_smart_scheduler' process.
-spec start_link(atom(), module(), term(), options()) ->
    {ok, pid()} | {error, term()}.
start_link(Name, Scheduler, SchedulerOptions, Options) ->
    erl_mesos_scheduler:start_link(Name, ?MODULE,
                                   {Name, Scheduler, SchedulerOptions, Options},
                                   Options).

%% @doc Teardown call.
-spec teardown(erl_mesos_scheduler:scheduler_info()) -> ok.
teardown(SchedulerInfo) ->
    send_queue_call(SchedulerInfo, teardown, []).

%% @equiv accept(SchedulerInfo, OfferIds, Operations, undefined)
-spec accept(erl_mesos_scheduler:scheduler_info(), [erl_mesos:'OfferID'()],
             [erl_mesos:'Offer.Operation'()]) ->
    ok.
accept(SchedulerInfo, OfferIds, Operations) ->
    accept(SchedulerInfo, OfferIds, Operations, undefined).

%% @doc Accept call.
-spec accept(erl_mesos_scheduler:scheduler_info(), [erl_mesos:'OfferID'()],
             [erl_mesos:'Offer.Operation'()],
             undefined | erl_mesos:'Filters'()) ->
    ok.
accept(SchedulerInfo, OfferIds, Operations, Filters) ->
    send_queue_call(SchedulerInfo, accept, [OfferIds, Operations, Filters]).

%% @equiv accept_inverse_offers(SchedulerInfo, OfferIds, undefined)
-spec accept_inverse_offers(erl_mesos_scheduler:scheduler_info(),
                            [erl_mesos:'OfferID'()]) ->
    ok.
accept_inverse_offers(SchedulerInfo, OfferIds) ->
    accept_inverse_offers(SchedulerInfo, OfferIds, undefined).

%% @doc Accept inverse offers call.
-spec accept_inverse_offers(erl_mesos_scheduler:scheduler_info(),
                            [erl_mesos:'OfferID'()],
                            undefined | erl_mesos:'Filters'()) ->
    ok.
accept_inverse_offers(SchedulerInfo, OfferIds, Filters) ->
    send_queue_call(SchedulerInfo, accept_inverse_offers, [OfferIds, Filters]).

%% @equiv decline(SchedulerInfo, OfferIds, undefined)
-spec decline(erl_mesos_scheduler:scheduler_info(),
              [erl_mesos:'OfferID'()]) ->
    ok.
decline(SchedulerInfo, OfferIds) ->
    decline(SchedulerInfo, OfferIds, undefined).

%% @doc Decline call.
-spec decline(erl_mesos_scheduler:scheduler_info(), [erl_mesos:'OfferID'()],
              undefined | erl_mesos:'Filters'()) ->
    ok.
decline(SchedulerInfo, OfferIds, Filters) ->
    send_queue_call(SchedulerInfo, decline, [OfferIds, Filters]).

%% @equiv decline_inverse_offers(SchedulerInfo, OfferIds, undefined)
-spec decline_inverse_offers(erl_mesos_scheduler:scheduler_info(),
                             [erl_mesos:'OfferID'()]) ->
    ok.
decline_inverse_offers(SchedulerInfo, OfferIds) ->
    decline_inverse_offers(SchedulerInfo, OfferIds, undefined).

%% @doc Decline inverse offers call.
-spec decline_inverse_offers(erl_mesos_scheduler:scheduler_info(),
                             [erl_mesos:'OfferID'()],
                             undefined | erl_mesos:'Filters'()) ->
    ok.
decline_inverse_offers(SchedulerInfo, OfferIds, Filters) ->
    send_queue_call(SchedulerInfo, decline_inverse_offers, [OfferIds, Filters]).

%% @doc Revive call.
-spec revive(erl_mesos_scheduler:scheduler_info()) -> ok.
revive(SchedulerInfo) ->
    send_queue_call(SchedulerInfo, revive, []).

%% @equiv kill(SchedulerInfo, TaskId, undefined)
-spec kill(erl_mesos_scheduler:scheduler_info(), erl_mesos:'TaskID'()) -> ok.
kill(SchedulerInfo, TaskId) ->
    kill(SchedulerInfo, TaskId, undefined).

%% @doc Kill call.
-spec kill(erl_mesos_scheduler:scheduler_info(), erl_mesos:'TaskID'(),
           undefined | 'AgentID()') ->
    ok.
kill(SchedulerInfo, TaskId, AgentId) ->
    send_queue_call(SchedulerInfo, revive, [TaskId, AgentId]).

%% @equiv shutdown(SchedulerInfo, ExecutorId, undefined)
-spec shutdown(erl_mesos_scheduler:scheduler_info(),
               erl_mesos:'ExecutorID'()) -> ok.
shutdown(SchedulerInfo, ExecutorId) ->
    shutdown(SchedulerInfo, ExecutorId, undefined).

%% @doc Shutdown call.
-spec shutdown(erl_mesos_scheduler:scheduler_info(), erl_mesos:'ExecutorID'(),
               undefined | erl_mesos:'AgentID'()) ->
    ok.
shutdown(SchedulerInfo, ExecutorId, AgentId) ->
    send_queue_call(SchedulerInfo, shutdown, [ExecutorId, AgentId]).

%% @doc Acknowledge call.
-spec acknowledge(erl_mesos_scheduler:scheduler_info(),
                  erl_mesos:'AgentID'(), erl_mesos:'TaskID'(), binary()) ->
    ok.
acknowledge(SchedulerInfo, AgentId, TaskId, Uuid) ->
    send_queue_call(SchedulerInfo, acknowledge, [AgentId, TaskId, Uuid]).

%% @doc Reconcile call.
-spec reconcile(erl_mesos_scheduler:scheduler_info(),
                [erl_mesos_scheduler:'Call.Reconcile.Task'()]) ->
    ok.
reconcile(SchedulerInfo, CallReconcileTasks) ->
    send_queue_call(SchedulerInfo, reconcile, [CallReconcileTasks]).

%% @doc Message call.
-spec message(erl_mesos_scheduler:scheduler_info(), erl_mesos:'AgentID'(),
              erl_mesos:'ExecutorID'(), binary()) ->
    ok.
message(SchedulerInfo, AgentId, ExecutorId, Data) ->
    send_queue_call(SchedulerInfo, message, [AgentId, ExecutorId, Data]).

%% @doc Request call.
-spec request(erl_mesos_scheduler:scheduler_info(),
              [erl_mesos:'Request'()]) ->
    ok.
request(SchedulerInfo, Requests) ->
    send_queue_call(SchedulerInfo, request, [Requests]).

%% @doc Suppress call.
-spec suppress(erl_mesos_scheduler:scheduler_info()) -> ok.
suppress(SchedulerInfo) ->
    send_queue_call(SchedulerInfo, suppress, []).

%% erl_mesos_scheduler callback functions.

-spec registered(erl_mesos_scheduler:scheduler_info(),
                 erl_mesos_scheduler:'Event.Subscribed'(), state()) ->
    {ok, state()} | {stop, state()}.
registered(SchedulerInfo, EventSubscribed, State) ->
    call(registered, SchedulerInfo, EventSubscribed, State).

-spec disconnected(erl_mesos_scheduler:scheduler_info(), state()) ->
    {ok, state()} | {stop, state()}.
disconnected(SchedulerInfo, State) ->
    call(disconnected, SchedulerInfo, State).

-spec reregistered(erl_mesos_scheduler:scheduler_info(), state()) ->
    {ok, state()} | {stop, state()}.
reregistered(SchedulerInfo, State) ->
    case execute_call(SchedulerInfo, State) of
        {ok, State1} ->
            call(reregistered, SchedulerInfo, State1);
        {stop, State1} ->
            {stop, State1}
    end.

-spec resource_offers(erl_mesos_scheduler:scheduler_info(),
                      erl_mesos_scheduler:'Event.Offers'(), state()) ->
    {ok, state()} | {stop, state()}.
resource_offers(SchedulerInfo, EventOffers, State) ->
    call(resource_offers, SchedulerInfo, EventOffers, State).

-spec resource_inverse_offers(erl_mesos_scheduler:scheduler_info(),
                              erl_mesos_scheduler:'Event.InverseOffers'(),
                              state()) ->
    {ok, state()} | {stop, state()}.
resource_inverse_offers(SchedulerInfo, EventInverseOffers, State) ->
    call(resource_inverse_offers, SchedulerInfo, EventInverseOffers, State).

-spec offer_rescinded(erl_mesos_scheduler:scheduler_info(),
                      erl_mesos_scheduler:'Event.Rescind'(), state()) ->
    {ok, state()} | {stop, state()}.
offer_rescinded(SchedulerInfo, EventRescind, State) ->
    call(offer_rescinded, SchedulerInfo, EventRescind, State).

-spec inverse_offer_rescinded(erl_mesos_scheduler:scheduler_info(),
                              erl_mesos_scheduler:'Event.RescindInverseOffer'(),
                              state()) ->
    {ok, state()} | {stop, state()}.
inverse_offer_rescinded(SchedulerInfo, EventRescindInverseOffer, State) ->
    call(inverse_offer_rescinded, SchedulerInfo, EventRescindInverseOffer,
         State).

-spec status_update(erl_mesos_scheduler:scheduler_info(),
                    erl_mesos_scheduler:'Event.Update'(), state()) ->
    {ok, state()} | {stop, state()}.
status_update(SchedulerInfo, EventUpdate, State) ->
    call(status_update, SchedulerInfo, EventUpdate, State).

-spec framework_message(erl_mesos_scheduler:scheduler_info(),
                        erl_mesos_scheduler:'Event.Message'(), state()) ->
    {ok, state()} | {stop, state()}.
framework_message(SchedulerInfo, EventMessage, State) ->
    call(framework_message, SchedulerInfo, EventMessage, State).

-spec slave_lost(erl_mesos_scheduler:scheduler_info(),
                 erl_mesos_scheduler:'Event.Failure'(), state()) ->
    {ok, state()} | {stop, state()}.
slave_lost(SchedulerInfo, EventFailure, State) ->
    call(slave_lost, SchedulerInfo, EventFailure, State).

-spec executor_lost(erl_mesos_scheduler:scheduler_info(),
                    erl_mesos_scheduler:'Event.Failure'(), state()) ->
    {ok, state()} | {stop, state()}.
executor_lost(SchedulerInfo, EventFailure, State) ->
    call(executor_lost, SchedulerInfo, EventFailure, State).

-spec error(erl_mesos_scheduler:scheduler_info(),
            erl_mesos_scheduler:'Event.Error'(), state()) ->
    {ok, state()} | {stop, state()}.
error(SchedulerInfo, EventError, State) ->
    call(error, SchedulerInfo, EventError, State).

-spec handle_info(erl_mesos_scheduler:scheduler_info(), term(), state()) ->
    {ok, state()} | {stop, state()}.
handle_info(#scheduler_info{framework_id = FrameworkId} = SchedulerInfo,
            {queue_call, FrameworkId, Call, Args},
            #state{max_queue_length = MaxQueueLength,
                   queue = Queue} = State)
  when MaxQueueLength > length(Queue) ->
    State1 = State#state{queue = Queue ++ [{Call, Args}]},
    execute_call(SchedulerInfo, State1);
handle_info(#scheduler_info{framework_id = FrameworkId},
            {queue_call, FrameworkId, _Call, _Args},
            #state{name = Name} = State) ->
    error_logger:error_msg("Mesos smart scheduler: ~p; queue overflow.",
                           [Name]),
    {stop, State};
handle_info(#scheduler_info{framework_id = FrameworkId} = SchedulerInfo,
            {execute_call, FrameworkId}, State) ->
    execute_call(SchedulerInfo, State);
handle_info(SchedulerInfo, Info, State) ->
    call(handle_info, SchedulerInfo, Info, State).

-spec terminate(term(), state()) -> term().
terminate(Reason, #state{scheduler = Scheduler,
                         scheduler_state = SchedulerState}) ->
    Scheduler:terminate(Reason, SchedulerState).

%% Internal functions.

%% @doc Validates options and sets state.
%% @private
-spec init({atom(), module(), term(), options()}) ->
    {ok, erl_mesos:'FrameworkInfo'(), state()} | {stop, term()}.
init({Name, Scheduler, SchedulerOptions, Options}) ->
    Funs = [fun max_num_execute/1, fun max_queue_length/1],
    case options(Funs, Options, []) of
        {ok, Options1} ->
            case Scheduler:init(SchedulerOptions) of
                {ok, FrameworkInfo, SchedulerState} ->
                    State = state(Name, Scheduler, SchedulerState, Options1),
                    {ok, FrameworkInfo, State};
                {stop, Reason} ->
                    {stop, Reason}
            end;
        {error, Reason} ->
            {stop, Reason}
    end.

%% @doc Returns max num execute.
%% @private
-spec max_num_execute(options()) ->
    {ok, {max_num_execute, pos_integer()}} |
    {error, {bad_max_num_execute, term()}}.
max_num_execute(Options) ->
    case proplists:get_value(max_num_execute, Options,
                             ?DEFAULT_MAX_NUM_EXECUTE) of
        MaxNumExecute
          when is_integer(MaxNumExecute) andalso MaxNumExecute > 0 ->
            {ok, {max_num_execute, MaxNumExecute}};
        MaxNumExecute ->
            {error, {bad_max_num_execute, MaxNumExecute}}
    end.

%% @doc Returns max queue length.
%% @private
-spec max_queue_length(options()) ->
    {ok, {max_queue_length, pos_integer()}} |
    {error, {bad_max_queue_length, term()}}.
max_queue_length(Options) ->
    case proplists:get_value(max_queue_length, Options,
                             ?DEFAULT_MAX_QUEUE_LENGTH) of
        MaxQueueLength
          when is_integer(MaxQueueLength) andalso MaxQueueLength > 0 ->
            {ok, {max_queue_length, MaxQueueLength}};
        MaxQueueLength ->
            {error, {bad_max_queue_length, MaxQueueLength}}
    end.

%% @doc Returns validated options.
%% @private
-spec options([fun((options()) -> {ok, {atom(), term()}} | {error, term()})],
              options(), options()) ->
    {ok, options()} | {error, term()}.
options([Fun | Funs], Options, ValidOptions) ->
    case Fun(Options) of
        {ok, Option} ->
            options(Funs, Options, [Option | ValidOptions]);
        {error, Reason} ->
            {error, Reason}
    end;
options([], _Options, ValidOptions) ->
    {ok, ValidOptions}.

%% @doc Returns state.
%% @private
-spec state(atom(), module(), term(), options()) -> state().
state(Name, Scheduler, SchedulerState, Options) ->
    MaxNumExecute = proplists:get_value(max_num_execute, Options),
    MaxQueueLength = proplists:get_value(max_queue_length, Options),
    #state{name = Name,
           scheduler = Scheduler,
           scheduler_state = SchedulerState,
           max_num_execute = MaxNumExecute,
           max_queue_length = MaxQueueLength}.

%% @doc Calls Scheduler:Callback/2.
%% @private
-spec call(atom(), erl_mesos_scheduler:scheduler_info(), state()) ->
    {ok, state()} | {stop, state()}.
call(Callback, SchedulerInfo, #state{scheduler = Scheduler,
                                     scheduler_state = SchedulerState} =
                              State) ->
    case Scheduler:Callback(SchedulerInfo, SchedulerState) of
        {ok, SchedulerState1} ->
            {ok, State#state{scheduler_state = SchedulerState1}};
        {stop, SchedulerState1} ->
            {stop, State#state{scheduler_state = SchedulerState1}}
    end.

%% @doc Calls Scheduler:Callback/3.
%% @private
-spec call(atom(), erl_mesos_scheduler:scheduler_info(), term(), state()) ->
    {ok, state()} | {stop, state()}.
call(Callback, SchedulerInfo, Arg, #state{scheduler = Scheduler,
                                          scheduler_state = SchedulerState} =
                                   State) ->
    case Scheduler:Callback(SchedulerInfo, Arg, SchedulerState) of
        {ok, SchedulerState1} ->
            {ok, State#state{scheduler_state = SchedulerState1}};
        {stop, SchedulerState1} ->
            {stop, State#state{scheduler_state = SchedulerState1}}
    end.

%% @doc Sends private queue call message.
%% @private
-spec send_queue_call(erl_mesos_scheduler:scheduler_info(), atom(), [term()]) ->
    ok.
send_queue_call(#scheduler_info{name = Name, framework_id = FrameworkId}, Call,
                Args) ->
    Name ! {queue_call, FrameworkId, Call, Args},
    ok.

%% @doc Executes call from the queue.
%% @private
-spec execute_call(erl_mesos_scheduler:scheduler_info(), state()) ->
    {ok, state()} | {stop, state()}.
execute_call(SchedulerInfo, #state{name = Name,
                                   max_num_execute = MaxNumExecute,
                                   num_execute = MaxNumExecute,
                                   queue = [{Call, Args} | _Queue]} = State) ->
    Args1 = [SchedulerInfo | Args],
    error_logger:error_msg("Mesos smart scheduler: ~p; maximum number of "
                           "execute, call: ~p, args: ~p.", [Name, Call, Args1]),
    {stop, State};
execute_call(#scheduler_info{framework_id = FrameworkId} = SchedulerInfo,
             #state{num_execute = NumExecute,
                    queue = [{Call, Args} | Queue]} = State) ->
    Args1 = [SchedulerInfo | Args],
    case apply(erl_mesos_scheduler_call, Call, Args1) of
        ok ->
            self() ! {execute_call, FrameworkId},
            {ok, State#state{num_execute = 0, queue = Queue}};
        {error, _Reason} ->
            {ok, State#state{num_execute = NumExecute +1}}
    end;
execute_call(_SchedulerInfo, #state{queue = []} = State) ->
    {ok, State}.
