-module(erl_mesos_scheduler).

-behaviour(gen_server).

-include("erl_mesos.hrl").

-export([start_link/4]).

-export([teardown/1, teardown/2]).

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
                master_hosts :: [binary()],
                subscribe_req_options :: [{atom(), term()}],
                heartbeat_timeout_window :: pos_integer(),
                max_num_resubscribe :: non_neg_integer(),
                resubscribe_interval :: non_neg_integer(),
                framework_info :: undefined | framework_info(),
                force :: undefined | boolean(),
                scheduler_state :: undefined | term(),
                master_hosts_queue :: undefined | [binary()],
                master_host :: undefined | binary(),
                client_ref :: undefined | reference(),
                subscribe_state :: undefined | subscribe_state(),
                framework_id :: undefined | framework_id(),
                num_redirect = 0 :: non_neg_integer(),
                num_resubscribe = 0 :: non_neg_integer(),
                heartbeat_timeout :: pos_integer(),
                heartbeat_timer_ref :: undefined | reference(),
                resubscribe_timer_ref :: undefined | reference()}).

-record(subscribe_response, {status :: undefined | non_neg_integer(),
                             headers :: undefined | [{binary(), binary()}]}).

-type options() :: [{atom(), term()}].
-export_type([options/0]).

-type state() :: #state{}.

-type subscribe_response() :: subscribe_response().

-type subscribe_state() :: subscribe_response() | subscribed.

%% Callbacks.

-callback init(term()) ->
    {ok, framework_info(), boolean(), term()} | {stop, term()}.

-callback registered(scheduler_info(), subscribed_event(), term()) ->
    {ok, term()} | {stop, term()}.

-callback disconnected(scheduler_info(), term()) ->
    {ok, term()} | {stop, term()}.

-callback reregistered(scheduler_info(), term()) ->
    {ok, term()} | {stop, term()}.

-callback error(scheduler_info(), error_event(), term()) ->
    {ok, term()} | {stop, term()}.

-callback handle_info(scheduler_info(), term(), term()) ->
    {ok, term()} | {stop, term()}.

-callback terminate(scheduler_info(), term(), term()) -> term().

-define(DEFAULT_MASTER_HOSTS, [<<"localhost:5050">>]).

-define(DEFAULT_SUBSCRIBE_REQ_OPTIONS, []).

-define(DEFAULT_MAX_REDIRECT, 5).

-define(DEFAULT_HEARTBEAT_TIMEOUT_WINDOW, 5000).

-define(DEFAULT_MAX_NUM_RESUBSCRIBE, 1).

-define(DEFAULT_RESUBSCRIBE_INTERVAL, 0).

-define(DATA_FORMAT, json).

%% External functions.

%% @doc Starts the `erl_mesos_scheduler' process.
-spec start_link(term(), module(), term(), options()) ->
    {ok, pid()} | {error, term()}.
start_link(Ref, Scheduler, SchedulerOptions, Options) ->
    gen_server:start_link(?MODULE, {Ref, Scheduler, SchedulerOptions, Options},
                          []).

%% @equiv teardown(scheduler_info(), [])
-spec teardown(scheduler_info()) -> ok | {error, term()}.
teardown(Scheduler) ->
    teardown(Scheduler, []).

%% @doc Sends teardown request.
-spec teardown(scheduler_info(), erl_mesos_api:request_options()) ->
    ok | {error, term()}.
teardown(#scheduler_info{data_format = DataFormat,
                         master_host = MasterHost,
                         framework_id = FrameworkId}, Options) ->
    erl_mesos_api:teardown(DataFormat, MasterHost, Options, FrameworkId).

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
handle_info({hackney_response, ClientRef, Response},
            #state{client_ref = ClientRef} = State) ->
    handle_subscribe_response(Response, State);
handle_info({'DOWN', ClientRef, Reason},
            #state{client_ref = ClientRef} = State) ->
    log_error("** Client process crashed~n",
              "** Reason == ~p~n",
              [Reason],
              State),
    handle_unsubscribe(State);
handle_info({timeout, HeartbeatTimerRef, heartbeat},
            #state{subscribe_state = subscribed,
                   heartbeat_timer_ref = HeartbeatTimerRef} = State) ->
    log_error("** Heartbeat timeout occurred~n",
              "",
              [],
              State),
    handle_unsubscribe(State);
handle_info({timeout, HeartbeatTimerRef, heartbeat},
            #state{heartbeat_timer_ref = HeartbeatTimerRef} = State) ->
    {noreply, State};
handle_info({timeout, ResubscribeTimerRef, resubscribe},
            #state{subscribe_state = undefined,
                   resubscribe_timer_ref = ResubscribeTimerRef} = State) ->
    resubscribe(State);
handle_info({timeout, ResubscribeTimerRef, resubscribe},
            #state{resubscribe_timer_ref = ResubscribeTimerRef} = State) ->
    {noreply, State};
handle_info(Info, State) ->
    case call(handle_info, Info, State) of
        {ok, State1} ->
            {noreply, State1};
        {stop, State1} ->
            {stop, shutdown, State1}
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

%% @doc Returns master hosts.
%% @private
-spec master_hosts(options()) ->
    {ok, {master_hosts, [binary()]}} | {error, {bad_master_host, term()}} |
    {error, {bad_master_hosts, term()}}.
master_hosts(Options) ->
    case erl_mesos_options:get_value(master_hosts, Options,
                                     ?DEFAULT_MASTER_HOSTS) of
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

%% @doc Returns subscribe request options.
%% @private
-spec subscribe_req_options(options()) ->
    {ok, {subscribe_req_options, [{atom(), term()}]}} |
    {error, {bad_subscribe_req_options, term()}}.
subscribe_req_options(Options) ->
    case erl_mesos_options:get_value(subscribe_req_options, Options,
                                     ?DEFAULT_SUBSCRIBE_REQ_OPTIONS) of
        SubscribeReqOptions when is_list(SubscribeReqOptions) ->
            DeleteKeys = [async, recv_timeout, following_redirect],
            SubscribeReqOptions1 = [{async, once},
                                    {recv_timeout, infinity},
                                    {following_redirect, false}] ++
                erl_mesos_options:delete(DeleteKeys, SubscribeReqOptions),
            {ok, {subscribe_req_options, SubscribeReqOptions1}};
        SubscribeReqOptions ->
            {error, {bad_subscribe_req_options, SubscribeReqOptions}}
    end.

%% @doc Returns heartbeat timeout window.
%% @private
-spec heartbeat_timeout_window(options()) ->
    {ok, {heartbeat_timeout_window, non_neg_integer()}} |
    {error, {bad_heartbeat_timeout_window, term()}}.
heartbeat_timeout_window(Options) ->
    case erl_mesos_options:get_value(heartbeat_timeout_window, Options,
                                     ?DEFAULT_HEARTBEAT_TIMEOUT_WINDOW) of
        HeartbeatTimeoutWindow
          when is_integer(HeartbeatTimeoutWindow) andalso
               HeartbeatTimeoutWindow >= 0 ->
            {ok, {heartbeat_timeout_window, HeartbeatTimeoutWindow}};
        HeartbeatTimeoutWindow ->
            {error, {bad_heartbeat_timeout_window, HeartbeatTimeoutWindow}}
    end.

%% @doc Returns maximum numumber of resubscribe.
%% @private
-spec max_num_resubscribe(options()) ->
    {ok, {max_num_resubscribe, non_neg_integer() | infinity}} |
    {error, {bad_max_num_resubscribe, term()}}.
max_num_resubscribe(Options) ->
    case erl_mesos_options:get_value(max_num_resubscribe, Options,
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
    case erl_mesos_options:get_value(resubscribe_interval, Options,
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

%% @doc Validates options and sets options to the state.
%% @private
-spec init(term(), module(), term(), options()) ->
    {ok, state()} | {error, term()}.
init(Ref, Scheduler, SchedulerOptions, Options) ->
    Funs = [fun master_hosts/1,
            fun subscribe_req_options/1,
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

%% @doc Returns state.
%% @private
-spec state(term(), module(), options()) -> state().
state(Ref, Scheduler, Options) ->
    MasterHosts = erl_mesos_options:get_value(master_hosts, Options),
    SubscribeReqOptions = erl_mesos_options:get_value(subscribe_req_options,
                                                      Options),
    HeartbeatTimeoutWindow =
        erl_mesos_options:get_value(heartbeat_timeout_window, Options),
    MaxNumResubscribe = erl_mesos_options:get_value(max_num_resubscribe,
                                                    Options),
    ResubscribeInterval = erl_mesos_options:get_value(resubscribe_interval,
                                                      Options),
    #state{ref = Ref,
           scheduler = Scheduler,
           data_format = ?DATA_FORMAT,
           master_hosts = MasterHosts,
           subscribe_req_options = SubscribeReqOptions,
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
            {ok, State#state{framework_info = FrameworkInfo,
                             force = Force,
                             scheduler_state = SchedulerState,
                             master_hosts_queue = MasterHosts}};
        {stop, Reason} ->
            {stop, Reason}
    end.

%% @doc Sends subscribe requests.
%% @private
-spec subscribe(state()) -> {ok, state()} | {error, bad_hosts}.
subscribe(#state{data_format = DataFormat,
                 subscribe_req_options = SubscribeReqOptions,
                 framework_info = FrameworkInfo,
                 force = Force,
                 master_hosts_queue = [MasterHost | MasterHostsQueue]} =
          State) ->
    log_info("** Try to subscribe~n",
             "** Host == ~s~n",
             [MasterHost],
             State),
    case erl_mesos_api:subscribe(DataFormat, MasterHost, SubscribeReqOptions,
                                 FrameworkInfo, Force) of
        {ok, ClientRef} ->
            {ok, State#state{master_hosts_queue = MasterHostsQueue,
                             master_host = MasterHost,
                             client_ref = ClientRef}};
        {error, Reason} ->
            log_error("** Can not subscribe~n",
                      "** Host == ~s~n"
                      "** Error reason == ~p~n",
                      [MasterHost, Reason],
                      State),
            subscribe(State#state{master_hosts_queue = MasterHostsQueue})
    end;
subscribe(#state{master_hosts_queue = []}) ->
    {error, bad_hosts}.

%% @doc Handles subscribe response.
%% @private
-spec handle_subscribe_response(term(), state()) ->
    {noreply, state()} | {stop, term(), state()}.
handle_subscribe_response({status, Status, _Message},
                          #state{client_ref = ClientRef,
                                 subscribe_state = undefined} = State) ->
    SubscribeResponse = #subscribe_response{status = Status},
    hackney:stream_next(ClientRef),
    {noreply, State#state{subscribe_state = SubscribeResponse}};
handle_subscribe_response({headers, Headers},
                          #state{client_ref = ClientRef,
                                 subscribe_state =
                                 #subscribe_response{headers = undefined} =
                                 SubscribeResponse} = State) ->
    SubscribeResponse1 = SubscribeResponse#subscribe_response{headers =
                                                              Headers},
    hackney:stream_next(ClientRef),
    {noreply, State#state{subscribe_state = SubscribeResponse1}};
handle_subscribe_response(Body,
                          #state{data_format = DataFormat,
                                 subscribe_state =
                                 #subscribe_response{status = 200,
                                                     headers = Headers}} =
                          State) ->
    ContentType = proplists:get_value(<<"Content-Type">>, Headers),
    case erl_mesos_data_format:content_type(DataFormat) of
        ContentType when is_binary(Body) ->
            handle_events(Body, State);
        ContentType when Body =:= done ->
            {noreply, State};
        _ContentType ->
            log_error("** Invalid content type~n",
                      "** Content type == ~s~n",
                      [ContentType],
                      State),
            handle_unsubscribe(State)
    end;
handle_subscribe_response(Events,
                          #state{subscribe_state = subscribed} = State)
  when is_binary(Events) ->
    handle_events(Events, State);
handle_subscribe_response(_Body,
                          #state{subscribe_state =
                                 #subscribe_response{status = 307}} = State) ->
    handle_redirect(State);
handle_subscribe_response(Body,
                          #state{subscribe_state =
                                 #subscribe_response{status = Status}} =
                          State) ->
    log_error("** Invalid http resposne~n",
              "** Status == ~p~n"
              "** Body == ~s~n",
              [Status, Body],
              State),
    handle_unsubscribe(State);
handle_subscribe_response(done, State) ->
    log_error("** Connection closed~n",
              "",
              [],
              State),
    handle_unsubscribe(State);
handle_subscribe_response({error, Reason}, State) ->
    log_error("** Connection error~n",
              "** Reason == ~p~n",
              [Reason],
              State),
    handle_unsubscribe(State).

%% @doc Handles redirect.
%% @private
-spec handle_redirect(state()) ->
    {noreply, state()} | {stop, term(), state()}.
handle_redirect(#state{master_hosts = MasterHosts,
                       subscribe_req_options = SubscribeReqOptions,
                       master_hosts_queue = MasterHostsQueue,
                       master_host = MasterHost,
                       client_ref = ClientRef,
                       subscribe_state =
                       #subscribe_response{headers = Headers},
                       framework_id = FrameworkId,
                       num_redirect = NumRedirect} = State) ->
    case proplists:get_value(max_redirect, SubscribeReqOptions,
                             ?DEFAULT_MAX_REDIRECT) of
        NumRedirect when FrameworkId =:= undefined ->
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

%% @doc Calls subscribe/1 or resubscribe/2.
%% @private
-spec redirect(state()) -> {noreply, state()} | {stop, term(), state()}.
redirect(#state{framework_id = undefined} = State) ->
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

%% @doc Handles unsubscribe.
%% @private
-spec handle_unsubscribe(state()) ->
    {noreply, state()} | {stop, term(), state()}.
handle_unsubscribe(#state{client_ref = ClientRef,
                          framework_id = undefined} = State) ->
    close(ClientRef),
    case subscribe(State) of
        {ok, State1} ->
            State2 = State1#state{subscribe_state = undefined},
            {noreply, State2};
        {error, Reason} ->
            {stop, {shutdown, {subscribe, {error, Reason}}}, State}
    end;
handle_unsubscribe(#state{client_ref = ClientRef,
                          subscribe_state = subscribed} = State) ->
    close(ClientRef),
    State1 = State#state{client_ref = undefined,
                         subscribe_state = undefined},
    case call(disconnected, State1) of
        {ok, #state{master_hosts = MasterHosts,
                    master_host = MasterHost} = State2} ->
            MasterHostsQueue = lists:delete(MasterHost, MasterHosts),
            State3 = State2#state{master_hosts_queue = MasterHostsQueue},
            start_resubscribe_timer(State3);
        {stop, State1} ->
            {stop, shutdown, State1}
    end;
handle_unsubscribe(#state{client_ref = ClientRef} = State) ->
    close(ClientRef),
    State1 = State#state{client_ref = undefined,
                         subscribe_state = undefined},
    start_resubscribe_timer(State1).

%% @doc Start resubscribe timer.
%% @private
-spec start_resubscribe_timer(state()) ->
    {noreply, state()} | {stop, term(), state()}.
start_resubscribe_timer(#state{framework_info =
                               #framework_info{failover_timeout =
                                               undefined}} = State) ->
    {stop, {shutdown, {resubscribe, {error, {failover_timeout, undefined}}}},
     State};
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
    {noreply, set_resubscribe_timer(State1)};
start_resubscribe_timer(#state{num_resubscribe = NumResubscribe} = State) ->
    State1 = State#state{client_ref = undefined,
                         num_resubscribe = NumResubscribe + 1},
    {noreply, set_resubscribe_timer(State1)}.

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
resubscribe(#state{data_format = DataFormat,
                   master_host = MasterHost,
                   subscribe_req_options = SubscribeReqOptions,
                   framework_info = FrameworkInfo,
                   framework_id = FrameworkId} = State) ->
    log_info("** Try to resubscribe~n",
             "** Host == ~s~n",
             [MasterHost],
             State),
    case erl_mesos_api:resubscribe(DataFormat, MasterHost, SubscribeReqOptions,
                                   FrameworkInfo, FrameworkId) of
        {ok, ClientRef} ->
            {noreply, State#state{client_ref = ClientRef}};
        {error, Reason} ->
            log_error("** Can not resubscribe~n",
                      "** Host == ~s~n"
                      "** Error reason == ~p~n",
                      [MasterHost, Reason],
                      State),
            handle_unsubscribe(State)
    end.

%% @doc Handles list of events.
%% @private
-spec handle_events(binary(), state()) ->
    {noreply, state()} | {stop, term(), state()}.
handle_events(Events, #state{data_format = DataFormat,
                             client_ref = ClientRef} = State) ->
    Objs = erl_mesos_data_format:decode_events(DataFormat, Events),
    case apply_events(Objs, State) of
        {ok, State1} ->
            hackney:stream_next(ClientRef),
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
                        subscribe_state = SubscribeState,
                        framework_id = FrameworkId} = State) ->
    case erl_mesos_scheduler_event:parse_obj(Obj) of
        {subscribed, {#subscribed_event{framework_id = SubscribeFrameworkId} =
                      SubscribedEvent, HeartbeatTimeout}}
          when is_record(SubscribeState, subscribe_response),
               FrameworkId =:= undefined ->
            log_info("** Successfuly subscribed~n",
                     "** Host == ~s~n",
                     [MasterHost],
                     State),
            State1 = State#state{framework_id = SubscribeFrameworkId,
                                 heartbeat_timeout = HeartbeatTimeout},
            State2 = set_heartbeat_timer(set_subscribed(State1)),
            call(registered, SubscribedEvent, State2);
        {subscribed, {_SubscribedEvent, HeartbeatTimeout}}
          when is_record(SubscribeState, subscribe_response) ->
            log_info("** Successfuly resubscribed~n",
                     "** Host == ~s~n",
                     [MasterHost],
                     State),
            State1 = State#state{heartbeat_timeout = HeartbeatTimeout},
            State2 = set_heartbeat_timer(set_subscribed(State1)),
            call(reregistered, State2);
        {error, ErrorEvent} ->
            call(error, ErrorEvent, State);
        heartbeat ->
            {ok, set_heartbeat_timer(State)};
        Event ->
            io:format("New unhandled event arrived: ~p~n", [Event]),
            {ok, State}
    end.

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

%% @doc Returns scheduler info.
%% @private
-spec scheduler_info(state()) -> scheduler_info().
scheduler_info(#state{data_format = DataFormat,
                      master_host = MasterHost,
                      subscribe_state = SubscribeState,
                      framework_id = FrameworkId}) ->
    Subscribed = SubscribeState =:= subscribed,
    #scheduler_info{data_format = DataFormat,
                    master_host = MasterHost,
                    subscribed = Subscribed,
                    framework_id = FrameworkId}.

%% @doc Sets subscribe state.
%% @private
-spec set_subscribed(state()) -> state().
set_subscribed(State) ->
    State#state{master_hosts_queue = undefined,
                subscribe_state = subscribed,
                num_redirect = 0,
                num_resubscribe = 0}.

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

%% @doc Closes the subscribe connection.
%% @private
-spec close(undefined | reference()) -> ok | {error, term()}.
close(undefined) ->
    ok;
close(ClientRef) ->
    hackney:close(ClientRef).

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
    erl_mesos_logger:warning(Message ++
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
                    master_hosts = MasterHosts,
                    subscribe_req_options = SubscribeReqOptions,
                    heartbeat_timeout_window = HeartbeatTimeoutWindow,
                    max_num_resubscribe = MaxNumResubscribe,
                    resubscribe_interval = ResubscribeInterval,
                    framework_info = FrameworkInfo,
                    force = Force,
                    scheduler_state = SchedulerState,
                    master_hosts_queue = MasterHostsQueue,
                    master_host = MasterHost,
                    client_ref = ClientRef,
                    subscribe_state = SubscribeState,
                    framework_id = FrameworkId,
                    num_redirect = NumRedirect,
                    num_resubscribe = NumResubscribe,
                    heartbeat_timeout = HeartbeatTimeout,
                    heartbeat_timer_ref = HeartbeatTimerRef,
                    resubscribe_timer_ref = ResubscribeTimerRef}) ->
    State = [{data_format, DataFormat},
             {master_hosts, MasterHosts},
             {subscribe_req_options, SubscribeReqOptions},
             {heartbeat_timeout_window, HeartbeatTimeoutWindow},
             {max_num_resubscribe, MaxNumResubscribe},
             {resubscribe_interval, ResubscribeInterval},
             {framework_info, FrameworkInfo},
             {force, Force},
             {master_hosts_queue, MasterHostsQueue},
             {master_host, MasterHost},
             {client_ref, ClientRef},
             {subscribe_state, SubscribeState},
             {framework_id, FrameworkId},
             {num_redirect, NumRedirect},
             {num_resubscribe, NumResubscribe},
             {heartbeat_timeout, HeartbeatTimeout},
             {heartbeat_timer_ref, HeartbeatTimerRef},
             {resubscribe_timer_ref, ResubscribeTimerRef}],
    [{"Ref", Ref},
     {"Scheduler", Scheduler},
     {"Scheduler state", SchedulerState},
     {"State", State}].
