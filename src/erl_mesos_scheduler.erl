-module(erl_mesos_scheduler).

-behaviour(gen_server).

-include("erl_mesos.hrl").

-export([start_link/4]).

-export([teardown/1,
         accept/3,
         accept/4,
         decline/2,
         decline/3,
         reconcile/2]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         format_status/2]).

-record(state, {ref :: term(),
                scheduler :: module(),
                data_format :: erl_mesos_data_format:data_format(),
                api_version :: erl_mesos_scheduler_call:version(),
                master_hosts :: [binary()],
                request_options :: erl_mesos_http:options(),
                heartbeat_timeout_window :: pos_integer(),
                max_num_resubscribe :: non_neg_integer(),
                resubscribe_interval :: non_neg_integer(),
                call_subscribe :: undefined | erl_mesos:call_subscribe(),
                scheduler_state :: undefined | term(),
                master_hosts_queue :: undefined | [binary()],
                master_host :: undefined | binary(),
                client_ref :: undefined | erl_mesos_http:client_ref(),
                recv_timer_ref :: undefined | reference(),
                subscribe_state :: undefined | subscribe_state(),
                num_redirect = 0 :: non_neg_integer(),
                num_resubscribe = 0 :: non_neg_integer(),
                heartbeat_timeout :: undefined | pos_integer(),
                heartbeat_timer_ref :: undefined | reference(),
                resubscribe_timer_ref :: undefined | reference()}).

-record(subscribe_response, {status :: undefined | non_neg_integer(),
                             headers :: undefined | erl_mesos_http:headers()}).

-type options() :: [{atom(), term()}].
-export_type([options/0]).

-type state() :: #state{}.

-type subscribe_response() :: subscribe_response().

-type subscribe_state() :: subscribe_response() | subscribed.

%% Callbacks.

-callback init(term()) ->
    {ok, erl_mesos:framework_info(), boolean(), term()} | {stop, term()}.

-callback registered(erl_mesos:scheduler_info(),
                     erl_mesos:event_subscribed(), term()) ->
    {ok, term()} | {stop, term()}.

-callback disconnected(erl_mesos:scheduler_info(), term()) ->
    {ok, term()} | {stop, term()}.

-callback reregistered(erl_mesos:scheduler_info(), term()) ->
    {ok, term()} | {stop, term()}.

-callback resource_offers(erl_mesos:scheduler_info(),
                          erl_mesos:event_offers(), term()) ->
    {ok, term()} | {stop, term()}.

-callback offer_rescinded(erl_mesos:scheduler_info(),
                          erl_mesos:event_rescind(), term()) ->
    {ok, term()} | {stop, term()}.

-callback status_update(erl_mesos:scheduler_info(),
                        erl_mesos:event_update(), term()) ->
    {ok, term()} | {stop, term()}.

-callback framework_message(erl_mesos:scheduler_info(),
                            erl_mesos:event_message(), term()) ->
    {ok, term()} | {stop, term()}.

-callback slave_lost(erl_mesos:scheduler_info(),
                     erl_mesos:event_failure(), term()) ->
    {ok, term()} | {stop, term()}.

-callback executor_lost(erl_mesos:scheduler_info(),
                        erl_mesos:event_failure(), term()) ->
    {ok, term()} | {stop, term()}.

-callback error(erl_mesos:scheduler_info(), erl_mesos:event_error(), term()) ->
    {ok, term()} | {stop, term()}.

-callback handle_info(erl_mesos:scheduler_info(), term(), term()) ->
    {ok, term()} | {stop, term()}.

-callback terminate(erl_mesos:scheduler_info(), term(), term()) -> term().

-define(DEFAULT_MASTER_HOSTS, [<<"localhost:5050">>]).

-define(DEFAULT_REQUEST_OPTIONS, []).

-define(DEFAULT_RECV_TIMEOUT, 30000).

-define(DEFAULT_MAX_REDIRECT, 5).

-define(DEFAULT_HEARTBEAT_TIMEOUT_WINDOW, 5000).

-define(DEFAULT_MAX_NUM_RESUBSCRIBE, 1).

-define(DEFAULT_RESUBSCRIBE_INTERVAL, 0).

-define(DATA_FORMAT, json).

-define(API_VERSION, v1).

%% External functions.

%% @doc Starts the `erl_mesos_scheduler' process.
-spec start_link(term(), module(), term(), options()) ->
    {ok, pid()} | {error, term()}.
start_link(Ref, Scheduler, SchedulerOptions, Options) ->
    gen_server:start_link(?MODULE, {Ref, Scheduler, SchedulerOptions, Options},
                          []).

%% @doc Teardown call.
-spec teardown(erl_mesos:scheduler_info()) -> ok | {error, term()}.
teardown(SchedulerInfo) ->
    erl_mesos_scheduler_call:teardown(SchedulerInfo).

%% @equiv accept(SchedulerInfo, OfferIds, Operations, undefined)
-spec accept(erl_mesos:scheduler_info(), [erl_mesos:offer_id()],
             [erl_mesos:offer_operation()]) ->
    ok | {error, term()}.
accept(SchedulerInfo, OfferIds, Operations) ->
    accept(SchedulerInfo, OfferIds, Operations, undefined).

%% @doc Accept call.
-spec accept(erl_mesos:scheduler_info(), [erl_mesos:offer_id()],
             [erl_mesos:offer_operation()], undefined | erl_mesos:filters()) ->
    ok | {error, term()}.
accept(SchedulerInfo, OfferIds, Operations, Filters) ->
    CallAccept = #call_accept{offer_ids = OfferIds,
                              operations = Operations,
                              filters = Filters},
    erl_mesos_scheduler_call:accept(SchedulerInfo, CallAccept).

%% @equiv decline(SchedulerInfo, OfferIds, undefined)
-spec decline(erl_mesos:scheduler_info(), [erl_mesos:offer_id()]) ->
    ok | {error, term()}.
decline(SchedulerInfo, OfferIds) ->
    decline(SchedulerInfo, OfferIds, undefined).

%% @doc Decline call.
-spec decline(erl_mesos:scheduler_info(), [erl_mesos:offer_id()],
              undefined | erl_mesos:filters()) ->
    ok | {error, term()}.
decline(SchedulerInfo, OfferIds, Filters) ->
    CallDecline = #call_accept{offer_ids = OfferIds,
                               filters = Filters},
    erl_mesos_scheduler_call:accept(SchedulerInfo, CallDecline).

%% @doc Reconcile call.
-spec reconcile(erl_mesos:scheduler_info(),
                [erl_mesos:call_reconcile_task()]) ->
    ok | {error, term()}.
reconcile(SchedulerInfo, CallReconcileTasks) ->
    CallReconcile = #call_reconcile{tasks = CallReconcileTasks},
    erl_mesos_scheduler_call:reconcile(SchedulerInfo, CallReconcile).

%% gen_server callback functions.

%% @private
-spec init({term(), module(), term(), options()}) ->
    {ok, state()} | {stop, term()}.
init({Ref, Scheduler, SchedulerOptions, Options}) ->
    case init(Ref, Scheduler, SchedulerOptions, Options) of
        {ok, State} ->
            {ok, State};
        {error, Reason} ->
            {stop, Reason}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, state()) -> {noreply, state()}.
handle_call(Request, _From, State) ->
    log_warning("** Scheduler received unexpected call request~n",
                "** Request == ~p~n",
                [Request], State),
    {noreply, State}.

%% @private
-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Request, State) ->
    log_warning("** Scheduler received unexpected cast request~n",
                "** Request == ~p~n",
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
        undefined ->
            case Info of
                {'DOWN', ClientRef, Reason} ->
                    log_error("** Client process crashed~n",
                              "** Reason == ~p~n",
                              [Reason],
                              State),
                    handle_unsubscribe(State);
                {timeout, RecvTimerRef, recv}
                  when is_reference(RecvTimerRef),
                       SubscribeState =/= subscribed ->
                    log_error("** Receive timeout occurred~n",
                              "",
                              [],
                              State),
                    handle_unsubscribe(State);
                {timeout, RecvTimerRef, recv}
                  when is_reference(RecvTimerRef) ->
                    {noreply, State};
                {timeout, HeartbeatTimerRef, heartbeat}
                  when SubscribeState =:= subscribed ->
                    log_error("** Heartbeat timeout occurred~n",
                              "",
                              [],
                              State),
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
            end;
        {async_response, _ClientRef, done} ->
            {noreply, State};
        AsyncResponse ->
            log_warning("** Scheduler received unexpected async response~n",
                        "** Async response == ~p~n",
                        [AsyncResponse],
                        State),
            {noreply, State}
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
-spec init(term(), module(), term(), options()) ->
    {ok, state()} | {error, term()}.
init(Ref, Scheduler, SchedulerOptions, Options) ->
    Funs = [fun master_hosts/1,
            fun request_options/1,
            fun heartbeat_timeout_window/1,
            fun max_num_resubscribe/1,
            fun resubscribe_interval/1],
    case options(Funs, Options) of
        {ok, ValidOptions} ->
            State = state(Ref, Scheduler, ValidOptions),
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
-spec state(term(), module(), options()) -> state().
state(Ref, Scheduler, Options) ->
    MasterHosts = proplists:get_value(master_hosts, Options),
    RequestOptions = proplists:get_value(request_options, Options),
    HeartbeatTimeoutWindow = proplists:get_value(heartbeat_timeout_window,
                                                 Options),
    MaxNumResubscribe = proplists:get_value(max_num_resubscribe, Options),
    ResubscribeInterval = proplists:get_value(resubscribe_interval, Options),
    #state{ref = Ref,
           scheduler = Scheduler,
           data_format = ?DATA_FORMAT,
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
        {ok, FrameworkInfo, Force, SchedulerState}
          when is_record(FrameworkInfo, framework_info), is_boolean(Force) ->
            CallSubscribe = #call_subscribe{framework_info = FrameworkInfo,
                                            force = Force},
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
    log_info("** Try to subscribe~n",
             "** Host == ~s~n",
             [MasterHost],
             State),
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
            log_error("** Can not subscribe~n",
                      "** Host == ~s~n"
                      "** Error reason == ~p~n",
                      [MasterHost, Reason],
                      State),
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
-spec scheduler_info(state()) -> erl_mesos:scheduler_info().
scheduler_info(#state{data_format = DataFormat,
                      api_version = ApiVersion,
                      master_host = MasterHost,
                      request_options = RequestOptions,
                      call_subscribe =
                      #call_subscribe{framework_info =
                                      #framework_info{id = Id}},
                      subscribe_state = SubscribeState}) ->
    Subscribed = SubscribeState =:= subscribed,
    #scheduler_info{data_format = DataFormat,
                    api_version = ApiVersion,
                    master_host = MasterHost,
                    request_options = RequestOptions,
                    subscribed = Subscribed,
                    framework_id = Id}.

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
            handle_events(Body, State);
        _ContentType ->
            log_error("** Invalid content type~n",
                      "** Content type == ~s~n",
                      [ContentType],
                      State),
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
    log_error("** Invalid http response~n",
              "** Status == ~p~n"
              "** Body == ~s~n",
              [Status, Body],
              State),
    handle_unsubscribe(State);
handle_async_response(done, State) ->
    log_error("** Connection closed~n",
              "",
              [],
              State),
    handle_unsubscribe(State);
handle_async_response({error, Reason}, State) ->
    log_error("** Connection error~n",
              "** Reason == ~p~n",
              [Reason],
              State),
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
handle_events(Events, #state{data_format = DataFormat} = State) ->
    Objs = erl_mesos_data_format:decode_events(DataFormat, Events),
    case apply_events(Objs, State) of
        {ok, State1} ->
            {noreply, State1};
        {stop, State1} ->
            {stop, shutdown, State1}
    end.

%% @doc Applies list of events.
%% @private
-spec apply_events([erl_mesos_obj:data_obj()], state()) ->
    {ok, state()} | {stop, state()}.
apply_events([Obj | Objs], State) ->
    case apply_event(Obj, State) of
        {ok, State1} ->
            apply_events(Objs, State1);
        {stop, State1} ->
            {stop, State1}
    end;
apply_events([], State) ->
    {ok, State}.

%% @doc Applies event.
%% @private
-spec apply_event(erl_mesos_obj:data_obj(), state()) ->
    {ok, state()} | {stop, state()}.
apply_event(Obj, #state{master_host = MasterHost,
                        call_subscribe =
                        #call_subscribe{framework_info =
                                        #framework_info{id = Id} =
                                        FrameworkInfo} =
                        CallSubscribe,
                        subscribe_state = SubscribeState} = State) ->
    case erl_mesos_scheduler_event:parse_obj(Obj) of
        #event{type = subscribed,
               subscribed = #event_subscribed{framework_id = FrameworkId,
                                              heartbeat_interval_seconds =
                                                  HeartbeatIntervalSeconds} =
                            EventSubscribed}
          when is_record(SubscribeState, subscribe_response),
               Id =:= undefined ->
            log_info("** Successfully subscribed~n",
                     "** Host == ~s~n",
                     [MasterHost],
                     State),
            FrameworkInfo1 = FrameworkInfo#framework_info{id = FrameworkId},
            CallSubscribe1 = CallSubscribe#call_subscribe{framework_info =
                                                          FrameworkInfo1},
            HeartbeatTimeout = heartbeat_timeout(HeartbeatIntervalSeconds),
            State1 = State#state{call_subscribe = CallSubscribe1,
                                 heartbeat_timeout = HeartbeatTimeout},
            State2 = set_heartbeat_timer(State1),
            State3 = set_subscribed(State2),
            call(registered, EventSubscribed, State3);
        #event{type = subscribed,
               subscribed = #event_subscribed{heartbeat_interval_seconds =
                                              HeartbeatIntervalSeconds}}
          when is_record(SubscribeState, subscribe_response) ->
            log_info("** Successfully resubscribed~n",
                     "** Host == ~s~n",
                     [MasterHost],
                     State),
            HeartbeatTimeout = heartbeat_timeout(HeartbeatIntervalSeconds),
            State1 = State#state{heartbeat_timeout = HeartbeatTimeout},
            State2 = set_heartbeat_timer(State1),
            State3 = set_subscribed(State2),
            call(reregistered, State3);
        #event{type = offers, offers = EventOffers} ->
            call(resource_offers, EventOffers, State);
        #event{type = rescind, rescind = EventRescind} ->
            call(offer_rescinded, EventRescind, State);
        #event{type = update, update = EventUpdate} ->
            call(status_update, EventUpdate, State);
        #event{type = message, message = EventMessage} ->
            call(framework_message, EventMessage, State);
        #event{type = failure,
               failure = #event_failure{executor_id = undefined} =
               EventFailure} ->
            call(slave_lost, EventFailure, State);
        #event{type = failure, failure = EventFailure} ->
            call(executor_lost, EventFailure, State);
        #event{type = error, error = EventError} ->
            call(error, EventError, State);
        #event{type = heartbeat} ->
            {ok, set_heartbeat_timer(State)}
    end.

%% @doc Returns heartbeat timeout.
%% @private
-spec heartbeat_timeout(float()) -> pos_integer().
heartbeat_timeout(HeartbeatIntervalSeconds) ->
    trunc(HeartbeatIntervalSeconds * 1000).

%% @doc Sets heartbeat timer.
%% @private
-spec set_heartbeat_timer(state()) -> state().
set_heartbeat_timer(#state{heartbeat_timeout_window = HeartbeatTimeoutWindow,
                           heartbeat_timeout = HeartbeatTimeout,
                           heartbeat_timer_ref = HeartbeatTimerRef} =
                    State) ->
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

%% @doc Sets subscribe state.
%% @private
-spec set_subscribed(state()) -> state().
set_subscribed(State) ->
    State#state{master_hosts_queue = undefined,
                subscribe_state = subscribed,
                num_redirect = 0,
                num_resubscribe = 0}.

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
handle_unsubscribe(#state{call_subscribe =
                          #call_subscribe{framework_info =
                                          #framework_info{id = undefined}},
                          client_ref = ClientRef,
                          recv_timer_ref = RecvTimerRef} = State) ->
    close(ClientRef),
    case subscribe(State) of
        {ok, State1} ->
            cancel_recv_timer(RecvTimerRef),
            State2 = State1#state{subscribe_state = undefined},
            {noreply, State2};
        {error, Reason} ->
            {stop, {shutdown, {subscribe, {error, Reason}}}, State}
    end;
handle_unsubscribe(#state{client_ref = ClientRef,
                          recv_timer_ref = RecvTimerRef,
                          subscribe_state = subscribed} = State) ->
    close(ClientRef),
    State1 = State#state{client_ref = undefined,
                         subscribe_state = undefined},
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
handle_unsubscribe(#state{client_ref = ClientRef,
                          recv_timer_ref = RecvTimerRef} = State) ->
    close(ClientRef),
    cancel_recv_timer(RecvTimerRef),
    State1 = State#state{client_ref = undefined,
                         subscribe_state = undefined},
    start_resubscribe_timer(State1).

%% @doc Start resubscribe timer.
%% @private
-spec start_resubscribe_timer(state()) ->
    {noreply, state()} | {stop, term(), state()}.
start_resubscribe_timer(#state{call_subscribe =
                               #call_subscribe{framework_info =
                                   #framework_info{failover_timeout =
                                                   FailoverTimeout}}} =
                        State)
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
                               num_resubscribe = MaxNumResubscribe} = State) ->
    State1 = State#state{master_hosts_queue = MasterHostsQueue,
                         master_host = MasterHost,
                         client_ref = undefined,
                         num_resubscribe = 1},
    State2 = set_resubscribe_timer(State1),
    {noreply, State2};
start_resubscribe_timer(#state{num_resubscribe = NumResubscribe} = State) ->
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
    log_info("** Try to resubscribe~n",
             "** Host == ~s~n",
             [MasterHost],
             State),
    SchedulerInfo = scheduler_info(State),
    case erl_mesos_scheduler_call:subscribe(SchedulerInfo, CallSubscribe) of
        {ok, ClientRef} ->
            State1 = State#state{client_ref = ClientRef},
            State2 = set_recv_timer(State1),
            {noreply, State2};
        {error, Reason} ->
            log_error("** Can not resubscribe~n",
                      "** Host == ~s~n"
                      "** Error reason == ~p~n",
                      [MasterHost, Reason],
                      State),
            handle_unsubscribe(State)
    end.

%% @doc Handles redirect.
%% @private
-spec handle_redirect(state()) -> {noreply, state()} | {stop, term(), state()}.
handle_redirect(#state{master_hosts = MasterHosts,
                       request_options = RequestOptions,
                       master_hosts_queue = MasterHostsQueue,
                       master_host = MasterHost,
                       call_subscribe =
                       #call_subscribe{framework_info =
                                       #framework_info{id = Id}},
                       client_ref = ClientRef,
                       subscribe_state =
                       #subscribe_response{headers = Headers},
                       num_redirect = NumRedirect} = State) ->
    case proplists:get_value(max_redirect, RequestOptions,
                             ?DEFAULT_MAX_REDIRECT) of
        NumRedirect when Id =:= undefined ->
            {stop, {shutdown, {subscribe, {error, max_redirect}}}, State};
        NumRedirect ->
            {stop, {shutdown, {resubscribe, {error, max_redirect}}}, State};
        _MaxNumRedirect ->
            close(ClientRef),
            MasterHost1 = proplists:get_value(<<"Location">>, Headers),
            log_info("** Redirect~n",
                     "** Form host == ~s~n"
                     "** To host == ~s~n",
                     [MasterHost, MasterHost1],
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

%% @doc Calls subscribe/1 or resubscribe/1.
%% @private
-spec redirect(state()) -> {noreply, state()} | {stop, term(), state()}.
redirect(#state{call_subscribe =
                #call_subscribe{framework_info =
                                #framework_info{id = undefined}}} = State) ->
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
-spec log_info(string(), string(), list(), state()) -> ok.
log_info(Message, Format, Data, #state{ref = Ref, scheduler = Scheduler}) ->
    erl_mesos_logger:info(Message ++
                          "** Ref == ~p~n"
                          "** Scheduler == ~p~n" ++
                          Format,
                          [Ref, Scheduler] ++ Data).

%% @doc Logs warning.
%% @private
-spec log_warning(string(), string(), list(), state()) -> ok.
log_warning(Message, Format, Data, #state{ref = Ref, scheduler = Scheduler}) ->
    erl_mesos_logger:warning(Message ++
                             "** Ref == ~p~n"
                             "** Scheduler == ~p~n" ++
                             Format,
                             [Ref, Scheduler] ++ Data).

%% @doc Logs error.
%% @private
-spec log_error(string(), string(), list(), state()) -> ok.
log_error(Message, Format, Data, #state{ref = Ref, scheduler = Scheduler}) ->
    erl_mesos_logger:error(Message ++
                           "** Ref == ~p~n"
                           "** Scheduler == ~p~n" ++
                           Format,
                           [Ref, Scheduler] ++ Data).

%% @doc Formats state.
%% @private
-spec format_state(state()) -> [{string(), [{atom(), term()}]}].
format_state(#state{ref = Ref,
                    scheduler = Scheduler,
                    data_format = DataFormat,
                    api_version = ApiVersion,
                    master_hosts = MasterHosts,
                    request_options = RequestOptions,
                    heartbeat_timeout_window = HeartbeatTimeoutWindow,
                    max_num_resubscribe = MaxNumResubscribe,
                    resubscribe_interval = ResubscribeInterval,
                    call_subscribe = CallSubscribe,
                    scheduler_state = SchedulerState,
                    master_hosts_queue = MasterHostsQueue,
                    master_host = MasterHost,
                    client_ref = ClientRef,
                    recv_timer_ref = RecvTimerRef,
                    subscribe_state = SubscribeState,
                    num_redirect = NumRedirect,
                    num_resubscribe = NumResubscribe,
                    heartbeat_timeout = HeartbeatTimeout,
                    heartbeat_timer_ref = HeartbeatTimerRef,
                    resubscribe_timer_ref = ResubscribeTimerRef}) ->
    State = [{data_format, DataFormat},
             {api_version, ApiVersion},
             {master_hosts, MasterHosts},
             {request_options, RequestOptions},
             {heartbeat_timeout_window, HeartbeatTimeoutWindow},
             {max_num_resubscribe, MaxNumResubscribe},
             {resubscribe_interval, ResubscribeInterval},
             {call_subscribe, CallSubscribe},
             {master_hosts_queue, MasterHostsQueue},
             {master_host, MasterHost},
             {client_ref, ClientRef},
             {recv_timer_ref, RecvTimerRef},
             {subscribe_state, SubscribeState},
             {num_redirect, NumRedirect},
             {num_resubscribe, NumResubscribe},
             {heartbeat_timeout, HeartbeatTimeout},
             {heartbeat_timer_ref, HeartbeatTimerRef},
             {resubscribe_timer_ref, ResubscribeTimerRef}],
    [{"Ref", Ref},
     {"Scheduler", Scheduler},
     {"Scheduler state", SchedulerState},
     {"State", State}].
