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

-module(erl_mesos_scheduler).

-behaviour(gen_server).

-include("erl_mesos_scheduler_info.hrl").

-include("erl_mesos_scheduler_proto.hrl").

-export([start_link/4, stop/2]).

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
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         format_status/2]).

-record(state, {name :: atom(),
                scheduler :: module(),
                data_format :: erl_mesos_data_format:data_format(),
                data_format_module :: module(),
                api_version :: erl_mesos_scheduler_call:version(),
                master_hosts :: [binary()],
                request_options :: erl_mesos_http:options(),
                heartbeat_timeout_window :: pos_integer(),
                max_num_resubscribe :: non_neg_integer(),
                resubscribe_interval :: non_neg_integer(),
                registered = false :: boolean(),
                call_subscribe :: undefined | 'Call.Subscribe'(),
                scheduler_state :: undefined | term(),
                master_hosts_queue :: undefined | [binary()],
                master_host :: undefined | binary(),
                client_ref :: undefined | erl_mesos_http:client_ref(),
                recv_timer_ref :: undefined | reference(),
                subscribe_state :: undefined | subscribe_state(),
                stream_id :: undefined | binary(),
                num_redirect = 0 :: non_neg_integer(),
                num_resubscribe = 0 :: non_neg_integer(),
                heartbeat_timeout :: undefined | pos_integer(),
                heartbeat_timer_ref :: undefined | reference(),
                resubscribe_timer_ref :: undefined | reference()}).

-record(subscribe_response, {status :: undefined | non_neg_integer(),
                             headers :: undefined | erl_mesos_http:headers()}).

-type options() :: [{atom(), term()}].
-export_type([options/0]).

-type 'Call'() :: #'Call'{}.
-export_type(['Call'/0]).

-type 'Call.Subscribe'() :: #'Call.Subscribe'{}.
-export_type(['Call.Subscribe'/0]).

-type 'Call.Accept'() :: #'Call.Accept'{}.
-export_type(['Call.Accept'/0]).

-type 'Call.AcceptInverseOffers'() :: #'Call.AcceptInverseOffers'{}.
-export_type(['Call.AcceptInverseOffers'/0]).

-type 'Call.Decline'() :: #'Call.Decline'{}.
-export_type(['Call.Decline'/0]).

-type 'Call.DeclineInverseOffers'() :: #'Call.DeclineInverseOffers'{}.
-export_type(['Call.DeclineInverseOffers'/0]).

-type 'Call.Kill'() :: #'Call.Kill'{}.
-export_type(['Call.Kill'/0]).

-type 'Call.Shutdown'() :: #'Call.Shutdown'{}.
-export_type(['Call.Shutdown'/0]).

-type 'Call.Acknowledge'() :: #'Call.Acknowledge'{}.
-export_type(['Call.Acknowledge'/0]).

-type 'Call.Reconcile.Task'() :: #'Call.Reconcile.Task'{}.
-export_type(['Call.Reconcile.Task'/0]).

-type 'Call.Reconcile'() :: #'Call.Reconcile'{}.
-export_type(['Call.Reconcile'/0]).

-type 'Call.Message'() :: #'Call.Message'{}.
-export_type(['Call.Message'/0]).

-type 'Call.Req'() :: #'Call.Req'{}.
-export_type(['Call.Req'/0]).

-type 'Event'() :: #'Event'{}.
-export_type(['Event'/0]).

-type 'Event.Subscribed'() :: #'Event.Subscribed'{}.
-export_type(['Event.Subscribed'/0]).

-type 'Event.Offers'() :: #'Event.Offers'{}.
-export_type(['Event.Offers'/0]).

-type 'Event.InverseOffers'() :: #'Event.InverseOffers'{}.
-export_type(['Event.InverseOffers'/0]).

-type 'Event.Rescind'() :: #'Event.Rescind'{}.
-export_type(['Event.Rescind'/0]).

-type 'Event.RescindInverseOffer'() :: #'Event.RescindInverseOffer'{}.
-export_type(['Event.RescindInverseOffer'/0]).

-type 'Event.Update'() :: #'Event.Update'{}.
-export_type(['Event.Update'/0]).

-type 'Event.Message'() :: #'Event.Message'{}.
-export_type(['Event.Message'/0]).

-type 'Event.Failure'() :: #'Event.Failure'{}.
-export_type(['Event.Failure'/0]).

-type 'Event.Error'() :: #'Event.Error'{}.
-export_type(['Event.Error'/0]).

-type scheduler_info() :: #scheduler_info{}.
-export_type([scheduler_info/0]).

-type state() :: #state{}.

-type subscribe_response() :: subscribe_response().

-type subscribe_state() :: subscribe_response() | subscribed.

%% Callbacks.

-callback init(term()) ->
    {ok, erl_mesos:'FrameworkInfo'(), term()} | {stop, term()}.

-callback registered(scheduler_info(), 'Event.Subscribed'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback disconnected(scheduler_info(), term()) ->
    {ok, term()} | {stop, term()}.

-callback reregistered(scheduler_info(), term()) ->
    {ok, term()} | {stop, term()}.

-callback resource_offers(scheduler_info(), 'Event.Offers'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback resource_inverse_offers(scheduler_info(), 'Event.InverseOffers'(),
                                  term()) ->
    {ok, term()} | {stop, term()}.

-callback offer_rescinded(scheduler_info(), 'Event.Rescind'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback inverse_offer_rescinded(scheduler_info(),
                                  'Event.RescindInverseOffer'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback status_update(scheduler_info(), 'Event.Update'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback framework_message(scheduler_info(), 'Event.Message'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback slave_lost(scheduler_info(), 'Event.Failure'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback executor_lost(scheduler_info(), 'Event.Failure'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback error(scheduler_info(), 'Event.Error'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback handle_info(scheduler_info(), term(), term()) ->
    {ok, term()} | {stop, term()}.

-callback terminate(scheduler_info(), term(), term()) -> term().

-define(DEFAULT_MASTER_HOSTS, [<<"localhost:5050">>]).

-define(DEFAULT_REQUEST_OPTIONS, []).

-define(DEFAULT_RECV_TIMEOUT, 5000).

-define(DEFAULT_MAX_REDIRECT, 5).

-define(DEFAULT_HEARTBEAT_TIMEOUT_WINDOW, 5000).

-define(DEFAULT_MAX_NUM_RESUBSCRIBE, 1).

-define(DEFAULT_RESUBSCRIBE_INTERVAL, 0).

-define(DATA_FORMAT, protobuf).

-define(DATA_FORMAT_MODULE, erl_mesos_scheduler_proto).

-define(API_VERSION, v1).

-define(DEFAULT_HEARTBEAT_INTERVAL, 15.0).

%% External functions.

%% @doc Starts the `erl_mesos_scheduler' process.
-spec start_link(atom(), module(), term(), options()) ->
    {ok, pid()} | {error, term()}.
start_link(Name, Scheduler, SchedulerOptions, Options) ->
    gen_server:start_link({local, Name}, ?MODULE,
                          {Name, Scheduler, SchedulerOptions, Options}, []).

%% @doc Stops the `erl_mesos_scheduler' process.
-spec stop(atom(), timeout()) -> ok.
stop(Name, Timeout) ->
    gen_server:call(Name, stop, Timeout).

%% @doc Teardown call.
-spec teardown(scheduler_info()) -> ok | {error, term()}.
teardown(SchedulerInfo) ->
    erl_mesos_scheduler_call:teardown(SchedulerInfo).

%% @equiv accept(SchedulerInfo, OfferIds, Operations, undefined)
-spec accept(scheduler_info(), [erl_mesos:'OfferID'()],
             [erl_mesos:'Offer.Operation'()]) ->
    ok | {error, term()}.
accept(SchedulerInfo, OfferIds, Operations) ->
    accept(SchedulerInfo, OfferIds, Operations, undefined).

%% @doc Accept call.
-spec accept(scheduler_info(), [erl_mesos:'OfferID'()],
             [erl_mesos:'Offer.Operation'()],
             undefined | erl_mesos:'Filters'()) ->
    ok | {error, term()}.
accept(SchedulerInfo, OfferIds, Operations, Filters) ->
    CallAccept = #'Call.Accept'{offer_ids = OfferIds,
                                operations = Operations,
                                filters = Filters},
    erl_mesos_scheduler_call:accept(SchedulerInfo, CallAccept).

%% @equiv accept_inverse_offers(SchedulerInfo, OfferIds, undefined)
-spec accept_inverse_offers(scheduler_info(), [erl_mesos:'OfferID'()]) ->
    ok | {error, term()}.
accept_inverse_offers(SchedulerInfo, OfferIds) ->
    accept_inverse_offers(SchedulerInfo, OfferIds, undefined).

%% @doc Accept inverse offers call.
-spec accept_inverse_offers(scheduler_info(), [erl_mesos:'OfferID'()],
                            undefined | erl_mesos:'Filters'()) ->
    ok | {error, term()}.
accept_inverse_offers(SchedulerInfo, OfferIds, Filters) ->
    CallAcceptInverseOffers =
        #'Call.AcceptInverseOffers'{inverse_offer_ids = OfferIds,
                                    filters = Filters},
    erl_mesos_scheduler_call:accept_inverse_offers(SchedulerInfo,
                                                   CallAcceptInverseOffers).

%% @equiv decline(SchedulerInfo, OfferIds, undefined)
-spec decline(scheduler_info(), [erl_mesos:'OfferID'()]) ->
    ok | {error, term()}.
decline(SchedulerInfo, OfferIds) ->
    decline(SchedulerInfo, OfferIds, undefined).

%% @doc Decline call.
-spec decline(scheduler_info(), [erl_mesos:'OfferID'()],
              undefined | erl_mesos:'Filters'()) ->
    ok | {error, term()}.
decline(SchedulerInfo, OfferIds, Filters) ->
    CallDecline = #'Call.Decline'{offer_ids = OfferIds,
                                  filters = Filters},
    erl_mesos_scheduler_call:decline(SchedulerInfo, CallDecline).

%% @equiv decline_inverse_offers(SchedulerInfo, OfferIds, undefined)
-spec decline_inverse_offers(scheduler_info(), [erl_mesos:'OfferID'()]) ->
    ok | {error, term()}.
decline_inverse_offers(SchedulerInfo, OfferIds) ->
    decline_inverse_offers(SchedulerInfo, OfferIds, undefined).

%% @doc Decline inverse offers call.
-spec decline_inverse_offers(scheduler_info(), [erl_mesos:'OfferID'()],
                             undefined | erl_mesos:'Filters'()) ->
    ok | {error, term()}.
decline_inverse_offers(SchedulerInfo, OfferIds, Filters) ->
    CallDeclineInverseOffers =
        #'Call.DeclineInverseOffers'{inverse_offer_ids = OfferIds,
                                     filters = Filters},
    erl_mesos_scheduler_call:decline_inverse_offers(SchedulerInfo,
                                                    CallDeclineInverseOffers).

%% @doc Revive call.
-spec revive(scheduler_info()) -> ok | {error, term()}.
revive(SchedulerInfo) ->
    erl_mesos_scheduler_call:revive(SchedulerInfo).

%% @equiv kill(SchedulerInfo, TaskId, undefined)
-spec kill(scheduler_info(), erl_mesos:'TaskID'()) -> ok | {error, term()}.
kill(SchedulerInfo, TaskId) ->
    kill(SchedulerInfo, TaskId, undefined).

%% @doc Kill call.
-spec kill(scheduler_info(), erl_mesos:'TaskID'(), undefined | 'AgentID()') ->
    ok | {error, term()}.
kill(SchedulerInfo, TaskId, AgentId) ->
    CallKill = #'Call.Kill'{task_id = TaskId,
                            agent_id = AgentId},
    erl_mesos_scheduler_call:kill(SchedulerInfo, CallKill).

%% @equiv shutdown(SchedulerInfo, ExecutorId, undefined)
-spec shutdown(scheduler_info(), erl_mesos:'ExecutorID'()) ->
    ok | {error, term()}.
shutdown(SchedulerInfo, ExecutorId) ->
    shutdown(SchedulerInfo, ExecutorId, undefined).

%% @doc Shutdown call.
-spec shutdown(scheduler_info(), erl_mesos:'ExecutorID'(),
               undefined | erl_mesos:'AgentID'()) ->
    ok | {error, term()}.
shutdown(SchedulerInfo, ExecutorId, AgentId) ->
    CallShutdown = #'Call.Shutdown'{executor_id = ExecutorId,
                                    agent_id = AgentId},
    erl_mesos_scheduler_call:shutdown(SchedulerInfo, CallShutdown).

%% @doc Acknowledge call.
-spec acknowledge(scheduler_info(), erl_mesos:'AgentID'(), erl_mesos:'TaskID'(),
                  binary()) ->
    ok | {error, term()}.
acknowledge(SchedulerInfo, AgentId, TaskId, Uuid) ->
    CallAcknowledge = #'Call.Acknowledge'{agent_id = AgentId,
                                          task_id = TaskId,
                                          uuid = Uuid},
    erl_mesos_scheduler_call:acknowledge(SchedulerInfo, CallAcknowledge).

%% @doc Reconcile call.
-spec reconcile(scheduler_info(), ['Call.Reconcile.Task'()]) ->
    ok | {error, term()}.
reconcile(SchedulerInfo, CallReconcileTasks) ->
    CallReconcile = #'Call.Reconcile'{tasks = CallReconcileTasks},
    erl_mesos_scheduler_call:reconcile(SchedulerInfo, CallReconcile).

%% @doc Message call.
-spec message(scheduler_info(), erl_mesos:'AgentID'(), erl_mesos:'ExecutorID'(),
              binary()) ->
    ok | {error, term()}.
message(SchedulerInfo, AgentId, ExecutorId, Data) ->
    CallMessage = #'Call.Message'{agent_id = AgentId,
                                  executor_id = ExecutorId,
                                  data = Data},
    erl_mesos_scheduler_call:message(SchedulerInfo, CallMessage).

%% @doc Request call.
-spec request(scheduler_info(), [erl_mesos:'Request'()]) -> ok | {error, term()}.
request(SchedulerInfo, Requests) ->
    CallRequest = #'Call.Req'{requests = Requests},
    erl_mesos_scheduler_call:request(SchedulerInfo, CallRequest).

%% @doc Suppress call.
-spec suppress(scheduler_info()) -> ok | {error, term()}.
suppress(SchedulerInfo) ->
    erl_mesos_scheduler_call:suppress(SchedulerInfo).

%% gen_server callback functions.

%% @private
-spec init({atom(), module(), term(), options()}) ->
    {ok, state()} | {stop, term()}.
init({Name, Scheduler, SchedulerOptions, Options}) ->
    case init(Name, Scheduler, SchedulerOptions, Options) of
        {ok, State} ->
            {ok, State};
        {error, Reason} ->
            {stop, Reason}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, state()) ->
    {stop, normal, ok, state()} | {noreply, state()}.
handle_call(stop, _From, State) ->
    log_info("scheduler stopped", State),
    {stop, normal, ok, State};
handle_call(Request, _From, State) ->
    log_warning("scheduler received unexpected call request", "request: ~p.",
                [Request], State),
    {noreply, State}.

%% @private
-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Request, State) ->
    log_warning("scheduler received unexpected cast request", "request: ~p.",
                [Request], State),
    {noreply, State}.

%% @private
-spec handle_info(term(), state()) ->
    {noreply, state()} | {stop, term(), state()}.
handle_info(Info, #state{client_ref = ClientRef,
                         recv_timer_ref = RecvTimerRef,
                         subscribe_state = SubscribeState,
                         heartbeat_timer_ref = HeartbeatTimerRef,
                         resubscribe_timer_ref = ResubscribeTimerRef} =
                  State) ->
    case erl_mesos_http:async_response(Info) of
        {async_response, ClientRef, Response} ->
            handle_async_response(Response, State);
        {async_response, _ClientRef, _Response} ->
            {noreply, State};
        undefined ->
            case Info of
                {'DOWN', ClientRef, Reason} ->
                    log_error("client process crashed", "reason: ~p.",
                              [Reason], State),
                    handle_unsubscribe(State);
                {timeout, RecvTimerRef, recv}
                  when is_reference(RecvTimerRef),
                       SubscribeState =/= subscribed ->
                    log_error("receive timeout occurred", State),
                    handle_unsubscribe(State);
                {timeout, RecvTimerRef, recv}
                  when is_reference(RecvTimerRef) ->
                    {noreply, State};
                {timeout, HeartbeatTimerRef, heartbeat}
                  when SubscribeState =:= subscribed ->
                    log_error("heartbeat timeout occurred", State),
                    handle_unsubscribe(State);
                {timeout, HeartbeatTimerRef, heartbeat} ->
                    {noreply, State};
                {timeout, ResubscribeTimerRef, resubscribe}
                  when SubscribeState =:= undefined ->
                    resubscribe(State);
                {timeout, ResubscribeTimerRef, resubscribe} ->
                    {noreply, State};
                _Info ->
                    call_handle_info(Info, State)
            end
    end.

%% @private
-spec terminate(term(), state()) -> term().
terminate(Reason, #state{client_ref = ClientRef,
                         scheduler = Scheduler,
                         scheduler_state = SchedulerState} = State) ->
    close(ClientRef),
    SchedulerInfo = scheduler_info(State),
    Scheduler:terminate(SchedulerInfo, Reason, SchedulerState).

%% @private
-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
-spec format_status(normal | terminate, [{atom(), term()} | state()]) ->
    [{atom(), term()}].
format_status(normal, [_Dict, State]) ->
    [{data, [{"State", format_state(State)}]}];
format_status(terminate, [_Dict, State]) ->
    format_state(State).

%% Internal functions.

%% @doc Validates options and sets options to the state.
%% @private
-spec init(atom(), module(), term(), options()) ->
    {ok, state()} | {error, term()}.
init(Name, Scheduler, SchedulerOptions, Options) ->
    Funs = [fun master_hosts/1,
            fun request_options/1,
            fun heartbeat_timeout_window/1,
            fun max_num_resubscribe/1,
            fun resubscribe_interval/1],
    case options(Funs, Options) of
        {ok, ValidOptions} ->
            State = state(Name, Scheduler, ValidOptions),
            case init(SchedulerOptions, State) of
                {ok, State1} ->
                    subscribe(State1);
                {stop, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Returns master hosts.
%% @private
-spec master_hosts(options()) ->
    {ok, {master_hosts, [binary()]}} | {error, {bad_master_host, term()}} |
    {error, {bad_master_hosts, term()}}.
master_hosts(Options) ->
    case proplists:get_value(master_hosts, Options, ?DEFAULT_MASTER_HOSTS) of
        MasterHosts when is_list(MasterHosts) andalso length(MasterHosts) > 0 ->
            case master_hosts(MasterHosts, []) of
                {ok, ValidMasterHosts} ->
                    {ok, {master_hosts, ValidMasterHosts}};
                {error, MasterHost} ->
                    {error, {bad_master_host, MasterHost}}
            end;
        MasterHosts ->
            {error, {bad_master_hosts, MasterHosts}}
    end.

%% @doc Validates and converts master hosts.
%% @private
-spec master_hosts([term()], [binary()]) -> {ok, [binary()]} | {error, term()}.
master_hosts([MasterHost | MasterHosts], ValidMasterHosts)
  when is_binary(MasterHost) ->
    master_hosts(MasterHosts, [MasterHost | ValidMasterHosts]);
master_hosts([MasterHost | MasterHosts], ValidMasterHosts)
  when is_list(MasterHost) ->
    master_hosts(MasterHosts, [list_to_binary(MasterHost) | ValidMasterHosts]);
master_hosts([MasterHost | _MasterHosts], _ValidMasterHosts) ->
    {error, MasterHost};
master_hosts([], ValidMasterHosts) ->
    {ok, lists:reverse(ValidMasterHosts)}.

%% @doc Returns request options.
%% @private
-spec request_options(options()) ->
    {ok, {request_options, [{atom(), term()}]}} |
    {error, {bad_request_options, term()}}.
request_options(Options) ->
    case proplists:get_value(request_options, Options,
                             ?DEFAULT_REQUEST_OPTIONS) of
        RequestOptions when is_list(RequestOptions) ->
            {ok, {request_options, RequestOptions}};
        RequestOptions ->
            {error, {bad_request_options, RequestOptions}}
    end.

%% @doc Returns heartbeat timeout window.
%% @private
-spec heartbeat_timeout_window(options()) ->
    {ok, {heartbeat_timeout_window, non_neg_integer()}} |
    {error, {bad_heartbeat_timeout_window, term()}}.
heartbeat_timeout_window(Options) ->
    case proplists:get_value(heartbeat_timeout_window, Options,
                             ?DEFAULT_HEARTBEAT_TIMEOUT_WINDOW) of
        HeartbeatTimeoutWindow
          when is_integer(HeartbeatTimeoutWindow) andalso
               HeartbeatTimeoutWindow >= 0 ->
            {ok, {heartbeat_timeout_window, HeartbeatTimeoutWindow}};
        HeartbeatTimeoutWindow ->
            {error, {bad_heartbeat_timeout_window, HeartbeatTimeoutWindow}}
    end.

%% @doc Returns maximum number of resubscribe.
%% @private
-spec max_num_resubscribe(options()) ->
    {ok, {max_num_resubscribe, non_neg_integer() | infinity}} |
    {error, {bad_max_num_resubscribe, term()}}.
max_num_resubscribe(Options) ->
    case proplists:get_value(max_num_resubscribe, Options,
                             ?DEFAULT_MAX_NUM_RESUBSCRIBE) of
        MaxNumResubscribe
          when is_integer(MaxNumResubscribe) andalso MaxNumResubscribe >= 0 ->
            {ok, {max_num_resubscribe, MaxNumResubscribe}};
        MaxNumResubscribe ->
            {error, {bad_max_num_resubscribe, MaxNumResubscribe}}
    end.

%% @doc Returns resubscribe interval.
%% @private
-spec resubscribe_interval(options()) ->
    {ok, {resubscribe_interval, non_neg_integer()}} |
    {error, {bad_resubscribe_interval, term()}}.
resubscribe_interval(Options) ->
    case proplists:get_value(resubscribe_interval, Options,
                             ?DEFAULT_RESUBSCRIBE_INTERVAL) of
        ResubscribeInterval
          when is_integer(ResubscribeInterval) andalso
               ResubscribeInterval >= 0 ->
            {ok, {resubscribe_interval, ResubscribeInterval}};
        ResubscribeInterval ->
            {error, {bad_resubscribe_interval, ResubscribeInterval}}
    end.

%% @doc Returns validated options.
%% @private
-spec options([fun((options()) -> {ok, {atom(), term()}} | {error, term()})],
              options()) ->
    {ok, options()} | {error, term()}.
options(Funs, Options) when is_list(Options) ->
    options(Funs, Options, []);
options(_Funs, Options) ->
    {error, {bad_options, Options}}.

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
-spec state(atom(), module(), options()) -> state().
state(Name, Scheduler, Options) ->
    MasterHosts = proplists:get_value(master_hosts, Options),
    RequestOptions = proplists:get_value(request_options, Options),
    HeartbeatTimeoutWindow = proplists:get_value(heartbeat_timeout_window,
                                                 Options),
    MaxNumResubscribe = proplists:get_value(max_num_resubscribe, Options),
    ResubscribeInterval = proplists:get_value(resubscribe_interval, Options),
    #state{name = Name,
           scheduler = Scheduler,
           data_format = ?DATA_FORMAT,
           data_format_module = ?DATA_FORMAT_MODULE,
           api_version = ?API_VERSION,
           master_hosts = MasterHosts,
           request_options = RequestOptions,
           heartbeat_timeout_window = HeartbeatTimeoutWindow,
           max_num_resubscribe = MaxNumResubscribe,
           resubscribe_interval = ResubscribeInterval}.

%% @doc Calls Scheduler:init/1.
%% @private
-spec init(term(), state()) -> {ok, state()} | {stop, term()}.
init(SchedulerOptions, #state{master_hosts = MasterHosts,
                              scheduler = Scheduler} = State) ->
    case Scheduler:init(SchedulerOptions) of
        {ok, FrameworkInfo, SchedulerState}
          when is_record(FrameworkInfo, 'FrameworkInfo') ->
            CallSubscribe = #'Call.Subscribe'{framework_info = FrameworkInfo},
            {ok, State#state{call_subscribe = CallSubscribe,
                             scheduler_state = SchedulerState,
                             master_hosts_queue = MasterHosts}};
        {stop, Reason} ->
            {stop, Reason}
    end.

%% @doc Sends subscribe requests.
%% @private
-spec subscribe(state()) -> {ok, state()} | {error, bad_hosts}.
subscribe(#state{call_subscribe = CallSubscribe,
                 master_hosts_queue = [MasterHost | MasterHostsQueue]} =
          State) ->
    log_info("try to subscribe", "host: ~s.", [MasterHost], State),
    SchedulerInfo = scheduler_info(State),
    SchedulerInfo1 = SchedulerInfo#scheduler_info{master_host = MasterHost},
    case erl_mesos_scheduler_call:subscribe(SchedulerInfo1, CallSubscribe) of
        {ok, ClientRef} ->
            State1 = State#state{master_hosts_queue = MasterHostsQueue,
                                 master_host = MasterHost,
                                 client_ref = ClientRef},
            State2 = set_recv_timer(State1),
            {ok, State2};
        {error, Reason} ->
            log_error("can not subscribe", "host: ~s, error reason ~p.",
                      [MasterHost, Reason], State),
            State1 = State#state{master_hosts_queue = MasterHostsQueue},
            subscribe(State1)
    end;
subscribe(#state{master_hosts_queue = []}) ->
    {error, bad_hosts}.

%% @doc Sets recv timer.
%% @private
-spec set_recv_timer(state()) -> state().
set_recv_timer(#state{request_options = RequestOptions} = State) ->
    RecvTimeout = proplists:get_value(recv_timeout, RequestOptions,
                                      ?DEFAULT_RECV_TIMEOUT),
    set_recv_timer(RecvTimeout, State).

%% @doc Sets recv timer.
%% @private
-spec set_recv_timer(infinity, state()) -> state().
set_recv_timer(infinity, State) ->
    State;
set_recv_timer(Timeout, State) ->
    RecvTimerRef = erlang:start_timer(Timeout, self(), recv),
    State#state{recv_timer_ref = RecvTimerRef}.

%% @doc Returns scheduler info.
%% @private
-spec scheduler_info(state()) -> scheduler_info().
scheduler_info(#state{name = Name,
                      data_format = DataFormat,
                      data_format_module = DataFormatModule,
                      api_version = ApiVersion,
                      master_host = MasterHost,
                      request_options = RequestOptions,
                      call_subscribe =
                      #'Call.Subscribe'{framework_info =
                                        #'FrameworkInfo'{id = Id}},
                      subscribe_state = SubscribeState,
                      stream_id = StreamId}) ->
    Subscribed = SubscribeState =:= subscribed,
    #scheduler_info{name = Name,
                    data_format = DataFormat,
                    data_format_module = DataFormatModule,
                    api_version = ApiVersion,
                    master_host = MasterHost,
                    request_options = RequestOptions,
                    subscribed = Subscribed,
                    framework_id = Id,
                    stream_id = StreamId}.

%% @doc Handles async response.
%% @private
-spec handle_async_response(term(), state()) ->
    {noreply, state()} | {stop, term(), state()}.
handle_async_response({status, Status, _Message},
                      #state{subscribe_state = undefined} = State) ->
    SubscribeResponse = #subscribe_response{status = Status},
    {noreply, State#state{subscribe_state = SubscribeResponse}};
handle_async_response({headers, Headers},
                      #state{subscribe_state =
                             #subscribe_response{headers = undefined} =
                             SubscribeResponse} = State) ->
    SubscribeResponse1 = SubscribeResponse#subscribe_response{headers =
                                                              Headers},
    {noreply, State#state{subscribe_state = SubscribeResponse1}};
handle_async_response(Body,
                      #state{data_format = DataFormat,
                             recv_timer_ref = RecvTimerRef,
                             subscribe_state =
                             #subscribe_response{status = 200,
                                                 headers = Headers}} = State)
  when is_binary(Body) ->
    ContentType = proplists:get_value(<<"Content-Type">>, Headers),
    case erl_mesos_data_format:content_type(DataFormat) of
        ContentType ->
            cancel_recv_timer(RecvTimerRef),
            StreamId = proplists:get_value(<<"Mesos-Stream-Id">>, Headers),
            handle_events(Body, State#state{stream_id = StreamId});
        _ContentType ->
            log_error("invalid content type", "content type: ~s.",
                      [ContentType], State),
            handle_unsubscribe(State)
    end;
handle_async_response(Events, #state{subscribe_state = subscribed} = State)
  when is_binary(Events) ->
    handle_events(Events, State);
handle_async_response(_Body,
                      #state{recv_timer_ref = RecvTimerRef,
                             subscribe_state =
                             #subscribe_response{status = 307}} = State) ->
    cancel_recv_timer(RecvTimerRef),
    handle_redirect(State);
handle_async_response(Body,
                      #state{subscribe_state =
                             #subscribe_response{status = Status}} = State) ->
    log_error("invalid http response", "status: ~p, body: ~s.", [Status, Body],
              State),
    handle_unsubscribe(State);
handle_async_response(done, State) ->
    log_error("connection closed", State),
    handle_unsubscribe(State);
handle_async_response({error, Reason}, State) ->
    log_error("connection error", "reason: ~p.", [Reason], State),
    handle_unsubscribe(State).

%% @doc Cancels recv timer.
%% @private
-spec cancel_recv_timer(undefined | reference()) ->
    undefined | false | non_neg_integer().
cancel_recv_timer(undefined) ->
    undefined;
cancel_recv_timer(RecvTimerRef) ->
    erlang:cancel_timer(RecvTimerRef).

%% @doc Handles list of events.
%% @private
-spec handle_events(binary(), state()) ->
    {noreply, state()} | {stop, term(), state()}.
handle_events(Events, #state{data_format = DataFormat,
                             data_format_module = DataFormatModule} = State) ->
    Messages = erl_mesos_data_format:decode_events(DataFormat, DataFormatModule,
                                                   Events),
    case apply_events(Messages, State) of
        {ok, State1} ->
            {noreply, State1};
        {stop, State1} ->
            {stop, shutdown, State1}
    end.

%% @doc Applies list of events.
%% @private
-spec apply_events([erl_mesos_data_format:message()], state()) ->
    {ok, state()} | {stop, state()}.
apply_events([Message | Messages], State) ->
    case apply_event(Message, State) of
        {ok, State1} ->
            apply_events(Messages, State1);
        {stop, State1} ->
            {stop, State1}
    end;
apply_events([], State) ->
    {ok, State}.

%% @doc Applies event.
%% @private
-spec apply_event(erl_mesos_data_format:message(), state()) ->
    {ok, state()} | {stop, state()}.
apply_event(Message, #state{master_host = MasterHost,
                            registered = Registered,
                            subscribe_state = SubscribeState} = State) ->
    case Message of
        #'Event'{type = 'SUBSCRIBED',
                 subscribed = EventSubscribed}
          when is_record(SubscribeState, subscribe_response), not Registered ->
            log_info("successfully subscribed", "host: ~s.", [MasterHost],
                     State),
            {EventSubscribed1, State1} = set_subscribed(EventSubscribed, State),
            call(registered, EventSubscribed1, State1#state{registered = true});
        #'Event'{type = 'SUBSCRIBED',
                 subscribed = EventSubscribed}
          when is_record(SubscribeState, subscribe_response) ->
            log_info("successfully resubscribed", "host: ~s.", [MasterHost],
                     State),
            {_EventSubscribed, State1} = set_subscribed(EventSubscribed, State),
            call(reregistered, State1);
        #'Event'{type = 'OFFERS', offers = EventOffers} ->
            call(resource_offers, EventOffers, State);
        #'Event'{type = 'INVERSE_OFFERS',
                 inverse_offers = EventInverseOffers} ->
            call(resource_inverse_offers, EventInverseOffers, State);
        #'Event'{type = 'RESCIND', rescind = EventRescind} ->
            call(offer_rescinded, EventRescind, State);
        #'Event'{type = 'RESCIND_INVERSE_OFFER',
                 rescind_inverse_offer = EventRescindInverseOffer} ->
            call(inverse_offer_rescinded, EventRescindInverseOffer, State);
        #'Event'{type = 'UPDATE', update = EventUpdate} ->
            call(status_update, EventUpdate, State);
        #'Event'{type = 'MESSAGE', message = EventMessage} ->
            call(framework_message, EventMessage, State);
        #'Event'{type = 'FAILURE',
                 failure = #'Event.Failure'{executor_id = undefined} =
                 EventFailure} ->
            call(slave_lost, EventFailure, State);
        #'Event'{type = 'FAILURE', failure = EventFailure} ->
            call(executor_lost, EventFailure, State);
        #'Event'{type = 'ERROR', error = EventError} ->
            call(error, EventError, State);
        #'Event'{type = 'HEARTBEAT'} ->
            {ok, set_heartbeat_timer(State)}
    end.

%% @doc Sets subscribed state.
%% @private
-spec set_subscribed('Event.Subscribed'(), state()) ->
    {'Event.Subscribed'(), state()}.
set_subscribed(EventSubscribed, #state{call_subscribe = CallSubscribe} =
               State) ->
    CallSubscribe1 = set_framework_id(EventSubscribed, CallSubscribe),
    EventSubscribed1 =
        #'Event.Subscribed'{heartbeat_interval_seconds =
                            HeartbeatInterval} =
            set_default_heartbeat_interval(EventSubscribed),
    HeartbeatTimeout = heartbeat_timeout(HeartbeatInterval),
    State1 = State#state{call_subscribe = CallSubscribe1,
                         master_hosts_queue = undefined,
                         subscribe_state = subscribed,
                         num_redirect = 0,
                         num_resubscribe = 0,
                         heartbeat_timeout = HeartbeatTimeout},
    {EventSubscribed1, set_heartbeat_timer(State1)}.

%% @doc Sets framework id.
%% @private
-spec set_framework_id('Event.Subscribed'(), 'Call.Subscribe'()) ->
    'Call.Subscribe'().
set_framework_id(#'Event.Subscribed'{framework_id = FrameworkId},
                 #'Call.Subscribe'{framework_info =
                                       #'FrameworkInfo'{id = undefined} =
                                   FrameworkInfo} = CallSubscribe) ->
    FrameworkInfo1 = FrameworkInfo#'FrameworkInfo'{id = FrameworkId},
    CallSubscribe#'Call.Subscribe'{framework_info = FrameworkInfo1};
set_framework_id(_EventSubscribed, CallSubscribe) ->
    CallSubscribe.

%% @doc Sets default heartbeat interval.
%% @private
-spec set_default_heartbeat_interval('Event.Subscribed'()) ->
    'Event.Subscribed'().
set_default_heartbeat_interval(#'Event.Subscribed'{heartbeat_interval_seconds =
                                                   undefined} =
                               EventSubscribed) ->
    EventSubscribed#'Event.Subscribed'{heartbeat_interval_seconds =
                                       ?DEFAULT_HEARTBEAT_INTERVAL};
set_default_heartbeat_interval(EventSubscribed) ->
    EventSubscribed.

%% @doc Returns heartbeat timeout.
%% @private
-spec heartbeat_timeout(float()) -> pos_integer().
heartbeat_timeout(HeartbeatInterval) ->
    trunc(HeartbeatInterval * 1000).

%% @doc Sets heartbeat timer.
%% @private
-spec set_heartbeat_timer(state()) -> state().
set_heartbeat_timer(#state{heartbeat_timeout_window = HeartbeatTimeoutWindow,
                           heartbeat_timeout = HeartbeatTimeout,
                           heartbeat_timer_ref = HeartbeatTimerRef} = State) ->
    cancel_heartbeat_timer(HeartbeatTimerRef),
    Timeout = HeartbeatTimeout + HeartbeatTimeoutWindow,
    HeartbeatTimerRef1 = erlang:start_timer(Timeout, self(), heartbeat),
    State#state{heartbeat_timer_ref = HeartbeatTimerRef1}.

%% @doc Cancels heartbeat timer.
%% @private
-spec cancel_heartbeat_timer(undefined | reference()) ->
    undefined | false | non_neg_integer().
cancel_heartbeat_timer(undefined) ->
    undefined;
cancel_heartbeat_timer(HeartbeatTimerRef) ->
    erlang:cancel_timer(HeartbeatTimerRef).

%% @doc Calls Scheduler:Callback/2.
%% @private
-spec call(atom(), state()) -> {ok, state()} | {stop, state()}.
call(Callback, #state{scheduler = Scheduler,
                      scheduler_state = SchedulerState} = State) ->
    SchedulerInfo = scheduler_info(State),
    case Scheduler:Callback(SchedulerInfo, SchedulerState) of
        {ok, SchedulerState1} ->
            {ok, State#state{scheduler_state = SchedulerState1}};
        {stop, SchedulerState1} ->
            {stop, State#state{scheduler_state = SchedulerState1}}
    end.

%% @doc Calls Scheduler:Callback/3.
%% @private
-spec call(atom(), term(), state()) -> {ok, state()} | {stop, state()}.
call(Callback, Arg, #state{scheduler = Scheduler,
                           scheduler_state = SchedulerState} = State) ->
    SchedulerInfo = scheduler_info(State),
    case Scheduler:Callback(SchedulerInfo, Arg, SchedulerState) of
        {ok, SchedulerState1} ->
            {ok, State#state{scheduler_state = SchedulerState1}};
        {stop, SchedulerState1} ->
            {stop, State#state{scheduler_state = SchedulerState1}}
    end.

%% @doc Handles unsubscribe.
%% @private
-spec handle_unsubscribe(state()) ->
    {noreply, state()} | {stop, term(), state()}.
handle_unsubscribe(#state{registered = false,
                          client_ref = ClientRef,
                          recv_timer_ref = RecvTimerRef} = State) ->
    case subscribe(State) of
        {ok, State1} ->
            close(ClientRef),
            cancel_recv_timer(RecvTimerRef),
            State2 = State1#state{client_ref = undefined,
                                  subscribe_state = undefined},
            {noreply, State2};
        {error, Reason} ->
            {stop, {shutdown, {subscribe, {error, Reason}}}, State}
    end;
handle_unsubscribe(#state{recv_timer_ref = RecvTimerRef,
                          subscribe_state = subscribed} = State) ->
    State1 = State#state{subscribe_state = undefined,
                         stream_id = undefined},
    case call(disconnected, State1) of
        {ok, #state{master_hosts = MasterHosts,
                    master_host = MasterHost} = State2} ->
            cancel_recv_timer(RecvTimerRef),
            MasterHostsQueue = lists:delete(MasterHost, MasterHosts),
            State3 = State2#state{master_hosts_queue = MasterHostsQueue},
            start_resubscribe_timer(State3);
        {stop, State1} ->
            {stop, shutdown, State1}
    end;
handle_unsubscribe(#state{recv_timer_ref = RecvTimerRef} = State) ->
    cancel_recv_timer(RecvTimerRef),
    State1 = State#state{subscribe_state = undefined,
                         stream_id = undefined},
    start_resubscribe_timer(State1).

%% @doc Start resubscribe timer.
%% @private
-spec start_resubscribe_timer(state()) ->
    {noreply, state()} | {stop, term(), state()}.
start_resubscribe_timer(#state{call_subscribe =
                               #'Call.Subscribe'{framework_info =
                                   #'FrameworkInfo'{failover_timeout =
                                                    FailoverTimeout}}} = State)
  when FailoverTimeout =:= undefined; FailoverTimeout == 0 ->
    {stop, {shutdown, {resubscribe,
     {error, {failover_timeout, FailoverTimeout}}}}, State};
start_resubscribe_timer(#state{max_num_resubscribe = 0} = State) ->
    {stop, {shutdown, {resubscribe, {error, max_num_resubscribe}}}, State};
start_resubscribe_timer(#state{max_num_resubscribe = MaxNumResubscribe,
                               master_hosts_queue = [],
                               num_resubscribe = MaxNumResubscribe} = State) ->
    {stop, {shutdown, {resubscribe, {error, max_num_resubscribe}}}, State};
start_resubscribe_timer(#state{max_num_resubscribe = MaxNumResubscribe,
                               master_hosts_queue =
                               [MasterHost | MasterHostsQueue],
                               client_ref = ClientRef,
                               num_resubscribe = MaxNumResubscribe} = State) ->
    close(ClientRef),
    State1 = State#state{master_hosts_queue = MasterHostsQueue,
                         master_host = MasterHost,
                         client_ref = undefined,
                         num_resubscribe = 1},
    State2 = set_resubscribe_timer(State1),
    {noreply, State2};
start_resubscribe_timer(#state{client_ref = ClientRef,
                               num_resubscribe = NumResubscribe} = State) ->
    close(ClientRef),
    State1 = State#state{client_ref = undefined,
                         num_resubscribe = NumResubscribe + 1},
    State2 = set_resubscribe_timer(State1),
    {noreply, State2}.

%% @doc Sets resubscribe timer.
%% @private
-spec set_resubscribe_timer(state()) -> state().
set_resubscribe_timer(#state{resubscribe_interval = ResubscribeInterval} =
                      State) ->
    ResubscribeTimerRef = erlang:start_timer(ResubscribeInterval, self(),
                                             resubscribe),
    State#state{resubscribe_timer_ref = ResubscribeTimerRef}.

%% @doc Sends resubscribe requests.
%% @private
-spec resubscribe(state()) -> {noreply, state()} | {stop, term(), state()}.
resubscribe(#state{master_host = MasterHost,
                   call_subscribe = CallSubscribe} = State) ->
    log_info("try to resubscribe", "host: ~s.", [MasterHost], State),
    SchedulerInfo = scheduler_info(State),
    case erl_mesos_scheduler_call:subscribe(SchedulerInfo, CallSubscribe) of
        {ok, ClientRef} ->
            State1 = State#state{client_ref = ClientRef},
            State2 = set_recv_timer(State1),
            {noreply, State2};
        {error, Reason} ->
            log_error("can not resubscribe", "host: ~s, error reason: ~p.",
                      [MasterHost, Reason], State),
            handle_unsubscribe(State)
    end.

%% @doc Handles redirect.
%% @private
-spec handle_redirect(state()) -> {noreply, state()} | {stop, term(), state()}.
handle_redirect(#state{master_hosts = MasterHosts,
                       request_options = RequestOptions,
                       master_hosts_queue = MasterHostsQueue,
                       master_host = MasterHost,
                       registered = Registered,
                       client_ref = ClientRef,
                       subscribe_state =
                       #subscribe_response{headers = Headers},
                       num_redirect = NumRedirect} = State) ->
    case proplists:get_value(max_redirect, RequestOptions,
                             ?DEFAULT_MAX_REDIRECT) of
        NumRedirect when not Registered ->
            {stop, {shutdown, {subscribe, {error, max_redirect}}}, State};
        NumRedirect ->
            {stop, {shutdown, {resubscribe, {error, max_redirect}}}, State};
        _MaxNumRedirect ->
            close(ClientRef),
            MasterHost1 = redirect_master_host(Headers),
            log_info("redirect", "form: ~s, to: ~s.", [MasterHost, MasterHost1],
                     State),
            MasterHosts1 = [MasterHost1 | lists:delete(MasterHost1,
                                                       MasterHosts)],
            MasterHostsQueue1 = [MasterHost1 | lists:delete(MasterHost,
                                                            MasterHostsQueue)],
            State1 = State#state{master_hosts = MasterHosts1,
                                 master_hosts_queue = MasterHostsQueue1,
                                 subscribe_state = undefined,
                                 num_redirect = NumRedirect + 1},
            redirect(State1)
    end.

%% @doc Returns redirect master host.
%% @private
-spec redirect_master_host(erl_mesos_http:headers()) -> binary().
redirect_master_host(Headers) ->
    case proplists:get_value(<<"Location">>, Headers) of
        <<"//", MasterHost/binary>> ->
            [MasterHost1 | _Path] = binary:split(MasterHost, <<"/">>),
            MasterHost1;
        MasterHost ->
            MasterHost
    end.

%% @doc Calls subscribe/1 or resubscribe/1.
%% @private
-spec redirect(state()) -> {noreply, state()} | {stop, term(), state()}.
redirect(#state{registered = false} = State) ->
    case subscribe(State) of
        {ok, State1} ->
            {noreply, State1};
        {error, Reason} ->
            {stop, {shutdown, {subscribe, {error, Reason}}}, State}
    end;
redirect(#state{master_hosts_queue = [MasterHost | MasterHostsQueue]} =
         State) ->
    State1 = State#state{master_hosts_queue = MasterHostsQueue,
                         master_host = MasterHost,
                         num_resubscribe = 0},
    resubscribe(State1).

%% @doc Calls Scheduler:handle_info/3.
%% @private
-spec call_handle_info(term(), state()) ->
    {noreply, state()} | {stop, shutdown, state()}.
call_handle_info(Info, State) ->
    case call(handle_info, Info, State) of
        {ok, State1} ->
            {noreply, State1};
        {stop, State1} ->
            {stop, shutdown, State1}
    end.

%% @doc Closes the connection.
%% @private
-spec close(undefined | erl_mesos_http:client_ref()) -> ok | {error, term()}.
close(undefined) ->
    ok;
close(ClientRef) ->
    erl_mesos_http:close_async_response(ClientRef).

%% @doc Logs info.
%% @private
-spec log_info(string(), state()) -> ok.
log_info(Message, #state{name = Name}) ->
    error_logger:info_msg("Mesos scheduler: ~p, message: ~s.", [Name, Message]).

%% @doc Logs info.
%% @private
-spec log_info(string(), string(), [term()], state()) -> ok.
log_info(Message, Format, Data, #state{name = Name}) ->
    error_logger:info_msg("Mesos scheduler: ~p, message: ~s, " ++ Format,
                          [Name, Message | Data]).

%% @doc Logs warning.
%% @private
-spec log_warning(string(), string(), [term()], state()) -> ok.
log_warning(Message, Format, Data, #state{name = Name}) ->
    error_logger:warning_msg("Mesos scheduler: ~p, message: ~s, " ++ Format,
                             [Name, Message | Data]).

%% @doc Logs error.
%% @private
-spec log_error(string(), state()) -> ok.
log_error(Message, #state{name = Name}) ->
    error_logger:error_msg("Mesos scheduler: ~p, message: ~s.",
                           [Name, Message]).

%% @doc Logs error.
%% @private
-spec log_error(string(), string(), [term()], state()) -> ok.
log_error(Message, Format, Data, #state{name = Name}) ->
    error_logger:error_msg("Mesos scheduler: ~p, message: ~s, " ++ Format,
                           [Name, Message | Data]).

%% @doc Formats state.
%% @private
-spec format_state(state()) -> [{string(), [{atom(), term()}]}].
format_state(#state{name = Name,
                    scheduler = Scheduler,
                    data_format = DataFormat,
                    data_format_module = DataFormatModule,
                    api_version = ApiVersion,
                    master_hosts = MasterHosts,
                    request_options = RequestOptions,
                    heartbeat_timeout_window = HeartbeatTimeoutWindow,
                    max_num_resubscribe = MaxNumResubscribe,
                    resubscribe_interval = ResubscribeInterval,
                    registered = Registered,
                    call_subscribe = CallSubscribe,
                    scheduler_state = SchedulerState,
                    master_hosts_queue = MasterHostsQueue,
                    master_host = MasterHost,
                    client_ref = ClientRef,
                    recv_timer_ref = RecvTimerRef,
                    subscribe_state = SubscribeState,
                    stream_id = StreamId,
                    num_redirect = NumRedirect,
                    num_resubscribe = NumResubscribe,
                    heartbeat_timeout = HeartbeatTimeout,
                    heartbeat_timer_ref = HeartbeatTimerRef,
                    resubscribe_timer_ref = ResubscribeTimerRef}) ->
    State = [{data_format, DataFormat},
             {data_format_module, DataFormatModule},
             {api_version, ApiVersion},
             {master_hosts, MasterHosts},
             {request_options, RequestOptions},
             {heartbeat_timeout_window, HeartbeatTimeoutWindow},
             {max_num_resubscribe, MaxNumResubscribe},
             {resubscribe_interval, ResubscribeInterval},
             {registered, Registered},
             {call_subscribe, CallSubscribe},
             {master_hosts_queue, MasterHostsQueue},
             {master_host, MasterHost},
             {client_ref, ClientRef},
             {recv_timer_ref, RecvTimerRef},
             {subscribe_state, SubscribeState},
             {stream_id, StreamId},
             {num_redirect, NumRedirect},
             {num_resubscribe, NumResubscribe},
             {heartbeat_timeout, HeartbeatTimeout},
             {heartbeat_timer_ref, HeartbeatTimerRef},
             {resubscribe_timer_ref, ResubscribeTimerRef}],
    [{"Name", Name},
     {"Scheduler", Scheduler},
     {"Scheduler state", SchedulerState},
     {"State", State}].
