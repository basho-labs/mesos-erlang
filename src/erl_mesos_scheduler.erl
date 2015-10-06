-module(erl_mesos_scheduler).

-behaviour(gen_server).

-include("erl_mesos.hrl").

-export([start_link/3]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         format_status/2]).

-record(state, {scheduler :: module(),
                data_format :: erl_mesos_data_format:data_format(),
                heartbeat_timeout :: pos_integer(),
                master_host :: binary(),
                subscribe_req_options :: [{atom(), term()}],
                heartbeat_timeout_window :: pos_integer(),
                max_num_resubscribe :: non_neg_integer() | infinity,
                resubscribe_timeout :: non_neg_integer(),
                framework_info :: undefined | framework_info(),
                scheduler_state :: undefined | term(),
                client_ref :: undefined | reference(),
                subscribe_state :: undefined | subscribe_state(),
                heartbeat_timeout_ref :: undefined | reference(),
                framework_id :: undefined | framework_id(),
                num_resubscribe = 0 :: non_neg_integer(),
                resubscribe_timeout_ref :: undefined | reference()}).

-record(subscribe_response, {status :: undefined | non_neg_integer(),
                             headers :: undefined | [{binary(), binary()}]}).

-type options() :: [{atom(), term()}].
-export_type([options/0]).

-type state() :: #state{}.

-type subscribe_response() :: subscribe_response().

-type subscribe_state() :: subscribe_response() | subscribed.

%% Callbacks.

-callback init(term()) ->
    {ok, framework_info(), boolean(), term()}.

-callback registered(subscribed(), term()) ->
    {ok, term()} | {ok, term(), hibernate}.

-define(DEFAULT_MASTER_HOST, <<"localhost:5050">>).

-define(DEFAULT_SUBSCRIBE_REQ_OPTIONS, []).

-define(DEFAULT_HEARTBEAT_TIMEOUT_WINDOW, 5000).

-define(DEFAULT_MAX_NUM_RESUBSCRIBE, infinity).

-define(DEFAULT_RESUBSCRIBE_TIMEOUT, 0).

-define(DATA_FORMAT, json).

-define(HEARTBEAT_TIMEOUT, 15000).

-define(SUBSCRIBE_REQ_OPTIONS, [{async, once},
                                {recv_timeout, infinity},
                                {following_redirect, true}]).

%% External functions.

%% @doc Starts the `erl_mesos_scheduler' process.
-spec start_link(module(), term(), options()) -> {ok, pid()}.
start_link(Scheduler, SchedulerOptions, Options) ->
    gen_server:start_link(?MODULE, {Scheduler, SchedulerOptions, Options}, []).

%% gen_server callback functions.

%% @private
-spec init({module(), term(), options()}) -> {ok, state()}.
init({Scheduler, SchedulerOptions, Options}) ->
    case init(Scheduler, SchedulerOptions, Options) of
        {ok, State} ->
            {ok, State};
        {error, Reason} ->
            {stop, Reason}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, state()) -> {noreply, state()}.
handle_call(Request, _From, State) ->
    %% Log unexpceted call message here.
    io:format("== Unexpected call message ~p~n", [Request]),
    {noreply, State}.

%% @private
-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Request, State) ->
    %% Log unexpceted cast message here.
    io:format("== Unexpected cast message ~p~n", [Request]),
    {noreply, State}.

%% @private
-spec handle_info(term(), state()) ->
    {noreply, state()} | {stop, term(), state()}.
handle_info({hackney_response, ClientRef, Response},
            #state{client_ref = ClientRef} = State) ->
    handle_subscribe_response(Response, State);
handle_info({'DOWN', ClientRef, _Reason},
            #state{client_ref = ClientRef} = State) ->
    start_resubscribe_timer(State);
handle_info({timeout, HeartbeatTimeoutRef, heartbeat},
            #state{subscribe_state = subscribed,
                   heartbeat_timeout_ref = HeartbeatTimeoutRef} = State) ->
    start_resubscribe_timer(State);
handle_info({timeout, HeartbeatTimeoutRef, heartbeat},
            #state{heartbeat_timeout_ref = HeartbeatTimeoutRef} = State) ->
    {noreply, State};
handle_info({timeout, ResubscribeTimeoutRef, resubscribe},
            #state{subscribe_state = undefined,
                   resubscribe_timeout_ref = ResubscribeTimeoutRef} = State) ->
    case resubscribe(State) of
        {ok, State1} ->
            {noreply, State1};
        stop ->
            {stop, shutdown, State};
        {error, _Reason} ->
            {stop, shutdown, State}
    end;
handle_info({timeout, ResubscribeTimeoutRef, resubscribe},
            #state{resubscribe_timeout_ref = ResubscribeTimeoutRef} = State) ->
    {noreply, State};
handle_info(Request, State) ->
    %% Log unexpceted message here.
    io:format("== Unexpected message ~p~n", [Request]),
    {noreply, State}.

%% @private
-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

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

%% @doc Returns master host.
%% @private
-spec master_host(options()) ->
    {ok, {master_host, binary()}} | {error, {bad_master_host, term()}}.
master_host(Options) ->
    case erl_mesos_options:get_value(master_host, Options,
                                     ?DEFAULT_MASTER_HOST) of
        MasterHost when is_binary(MasterHost) ->
            {ok, {master_host, MasterHost}};
        MasterHost when is_list(MasterHost) ->
            {ok, {master_host, list_to_binary(MasterHost)}};
        MasterHost ->
            {error, {bad_master_host, MasterHost}}
    end.

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
            SubscribeReqOptions1 =
                erl_mesos_options:delete(DeleteKeys, SubscribeReqOptions),
            {ok, {subscribe_req_options,
                  ?SUBSCRIBE_REQ_OPTIONS ++ SubscribeReqOptions1}};
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
          when (is_integer(MaxNumResubscribe) andalso MaxNumResubscribe >=0)
               orelse MaxNumResubscribe =:= infinity ->
            {ok, {max_num_resubscribe, MaxNumResubscribe}};
        MaxNumResubscribe ->
            {error, {bad_max_num_resubscribe, MaxNumResubscribe}}
    end.

%% @doc Returns resubscribe timeout.
%% @private
-spec resubscribe_timeout(options()) ->
    {ok, {resubscribe_timeout, non_neg_integer()}} |
    {error, {bad_resubscribe_timeout, term()}}.
resubscribe_timeout(Options) ->
    case erl_mesos_options:get_value(resubscribe_timeout, Options,
                                     ?DEFAULT_RESUBSCRIBE_TIMEOUT) of
        ResubscribeTimeout
          when is_integer(ResubscribeTimeout) andalso ResubscribeTimeout >= 0 ->
            {ok, {resubscribe_timeout, ResubscribeTimeout}};
        ResubscribeTimeout ->
            {error, {bad_resubscribe_timeout, ResubscribeTimeout}}
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
-spec init(module(), term(), options()) -> {ok, state()} | {error, term()}.
init(Scheduler, SchedulerOptions, Options) ->
    Funs = [fun master_host/1,
            fun subscribe_req_options/1,
            fun heartbeat_timeout_window/1,
            fun max_num_resubscribe/1,
            fun resubscribe_timeout/1],
    case options(Funs, Options) of
        {ok, ValidOptions} ->
            State = state(Scheduler, ValidOptions),
            {ok, State1, Force} = init(SchedulerOptions, State),
            subscribe(State1, Force);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Returns state.
%% @private
-spec state(module(), options()) -> state().
state(Scheduler, Options) ->
    MasterHost = erl_mesos_options:get_value(master_host, Options),
    SubscribeReqOptions = erl_mesos_options:get_value(subscribe_req_options,
                                                      Options),
    HeartbeatTimeoutWindow =
        erl_mesos_options:get_value(heartbeat_timeout_window, Options),
    MaxNumResubscribe = erl_mesos_options:get_value(max_num_resubscribe,
                                                    Options),
    ResubscribeTimeout = erl_mesos_options:get_value(resubscribe_timeout,
                                                     Options),
    #state{scheduler = Scheduler,
           data_format = ?DATA_FORMAT,
           heartbeat_timeout = ?HEARTBEAT_TIMEOUT,
           master_host = MasterHost,
           subscribe_req_options = SubscribeReqOptions,
           heartbeat_timeout_window = HeartbeatTimeoutWindow,
           max_num_resubscribe = MaxNumResubscribe,
           resubscribe_timeout = ResubscribeTimeout}.

%% @doc Calls Scheduler:init/1.
%% @private
-spec init(term(), state()) -> {ok, state(), boolean()}.
init(SchedulerOptions, #state{scheduler = Scheduler} = State) ->
    {ok, FrameworkInfo, Force, SchedulerState} =
        Scheduler:init(SchedulerOptions),
    true = is_record(FrameworkInfo, framework_info),
    true = is_boolean(Force),
    {ok, State#state{framework_info = FrameworkInfo,
                     scheduler_state = SchedulerState}, Force}.

%% @doc Calls subscribe api request.
%% @private
-spec subscribe(state(), boolean()) -> {ok, state()} | {error, term()}.
subscribe(#state{data_format = DataFormat,
                 master_host = MasterHost,
                 subscribe_req_options = SubscribeReqOptions,
                 framework_info = FrameworkInfo} = State, Force) ->
    case erl_mesos_api:subscribe(DataFormat, MasterHost, SubscribeReqOptions,
                                 FrameworkInfo, Force) of
        {ok, ClientRef} ->
            {ok, State#state{client_ref = ClientRef}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Handles subscribe response.
%% @private
-spec handle_subscribe_response(term(), state()) -> {noreply, state()}.
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
handle_subscribe_response(done, State) ->
    start_resubscribe_timer(State);
handle_subscribe_response({error, _Reason}, State) ->
    start_resubscribe_timer(State);
handle_subscribe_response(Packets,
                          #state{subscribe_state =
                                 #subscribe_response{status = 200}} = State) ->
    handle_packets(Packets, State);
handle_subscribe_response(_Error,
                          #state{subscribe_state =
                                 #subscribe_response{status = _Status}} =
                          State) ->
    {stop, shutdown, State};
handle_subscribe_response(Packets,
                          #state{subscribe_state = subscribed} = State) ->
    handle_packets(Packets, State).

%% @doc Start resubscribe timer.
%% @private
-spec start_resubscribe_timer(state()) ->
    {noreply, state()} | {stop, shutdown, state()}.
start_resubscribe_timer(#state{framework_id = undefined} = State) ->
    {stop, shutdown, State};
start_resubscribe_timer(#state{framework_info =
                               #framework_info{failover_timeout = undefined}} =
                        State) ->
    {stop, shutdown, State};
start_resubscribe_timer(#state{max_num_resubscribe = NumResubscribe,
                               num_resubscribe = NumResubscribe} = State) ->
    {stop, shutdown, State};
start_resubscribe_timer(#state{client_ref = ClientRef,
                               num_resubscribe = NumResubscribe,
                               resubscribe_timeout = ResubscribeTimeout} =
                        State) ->
    hackney:close(ClientRef),
    ResubscribeTimeoutRef = erlang:start_timer(ResubscribeTimeout, self(),
                                               resubscribe),
    {noreply, State#state{client_ref = undefined,
                          subscribe_state = undefined,
                          num_resubscribe = NumResubscribe + 1,
                          resubscribe_timeout_ref = ResubscribeTimeoutRef}}.

%% @doc Calls resubscribe api request.
%% @private
-spec resubscribe(state()) -> {ok, state()} | stop | {error, term()}.
resubscribe(#state{framework_id = undefined}) ->
    stop;
resubscribe(#state{data_format = DataFormat,
                   master_host = MasterHost,
                   subscribe_req_options = SubscribeReqOptions,
                   framework_info = FrameworkInfo,
                   framework_id = FrameworkId} = State) ->
    case erl_mesos_api:resubscribe(DataFormat, MasterHost, SubscribeReqOptions,
                                   FrameworkInfo, FrameworkId) of
        {ok, ClientRef} ->
            {ok, State#state{client_ref = ClientRef,
                             resubscribe_timeout_ref = undefined}};
        {error, _Reason} ->
            {error, _Reason}
    end.

%% @doc Handle packets.
%% @private
-spec handle_packets(binary(), state()) -> {noreply, state()}.
handle_packets(Packets, #state{data_format = DataFormat,
                               client_ref = ClientRef} = State) ->
    DecodePackets = erl_mesos_data_format:decode_packets(DataFormat, Packets),
    {ok, State1} = parse_packets(DecodePackets, State),
    hackney:stream_next(ClientRef),
    {noreply, State1}.

%% @doc Parses packets.
%% @private
-spec parse_packets([erl_mesos_obj:data_obj()], state()) -> {ok, state()}.
parse_packets([Packet | Packets], State) ->
    {ok, State1} = parse_packet(Packet, State),
    parse_packets(Packets, State1);
parse_packets([], State) ->
    {ok, State}.

%% @doc Parses packet.
%% @private
-spec parse_packet(term(), state()) -> {ok, state()}.
parse_packet(Packet, #state{subscribe_state = SubscribeState,
                            framework_id = FrameworkId} = State) ->
    case erl_mesos_scheduler_packet:parse(Packet) of
        {subscribed_packet, #subscribed{framework_id = SubscribeFrameworkId} =
                            Subscribed}
          when is_record(SubscribeState, subscribe_response),
               FrameworkId =:= undefined ->
            State1 = State#state{subscribe_state = subscribed,
                                 framework_id = SubscribeFrameworkId},
            call(registered, Subscribed, set_heartbeat_timeout(State1));
        {subscribed_packet, _Subscribed}
          when is_record(SubscribeState, subscribe_response) ->
            State1 = State#state{subscribe_state = subscribed},
            {ok, set_heartbeat_timeout(State1)};
        heartbeat_packet ->
            {ok, set_heartbeat_timeout(State)};
        DecodePacket ->
            io:format("New unhandled packet arrived: ~p~n", [DecodePacket]),
            {ok, State}
    end.

%% @doc Sets heartbeat timeout.
%% @private
-spec set_heartbeat_timeout(state()) -> state().
set_heartbeat_timeout(#state{heartbeat_timeout = HeartbeatTimeout,
                             heartbeat_timeout_window = HeartbeatTimeoutWindow,
                             heartbeat_timeout_ref = HeartbeatTimeoutRef} =
                      State) ->
    case HeartbeatTimeoutRef of
        undefined ->
            ok;
        _HeartbeatTimeoutRef ->
            erlang:cancel_timer(HeartbeatTimeoutRef)
    end,
    Timeout = HeartbeatTimeout + HeartbeatTimeoutWindow,
    HeartbeatTimeoutRef1 = erlang:start_timer(Timeout, self(), heartbeat),
    State#state{heartbeat_timeout_ref = HeartbeatTimeoutRef1}.

%% @doc Calls Scheduler:Callback/2.
%% @private
-spec call(atom(), term(), state()) -> {ok, state()}.
call(Callback, Arg, #state{scheduler = Scheduler,
                           scheduler_state = SchedulerState} = State) ->
    {ok, SchedulerState1} = Scheduler:Callback(Arg, SchedulerState),
    {ok, State#state{scheduler_state = SchedulerState1}}.

%% @doc Formats state.
%% @private
-spec format_state(state()) -> [{string(), [{atom(), term()}]}].
format_state(#state{scheduler = Scheduler,
                    data_format = DataFormat,
                    heartbeat_timeout = HeartbeatTimeout,
                    master_host = MasterHost,
                    subscribe_req_options = SubscribeReqOptions,
                    heartbeat_timeout_window = HeartbeatTimeoutWindow,
                    max_num_resubscribe = MaxNumResubscribe,
                    resubscribe_timeout = ResubscribeTimeout,
                    framework_info = FrameworkInfo,
                    scheduler_state = SchedulerState,
                    client_ref = ClientRef,
                    subscribe_state = SubscribeState,
                    heartbeat_timeout_ref = HeartbeatTimeoutRef,
                    framework_id = FrameworkId,
                    num_resubscribe = NumResubscribe,
                    resubscribe_timeout_ref = ResubscribeTimeoutRef}) ->
    State = [{data_format, DataFormat},
             {heartbeat_timeout, HeartbeatTimeout},
             {master_host, MasterHost},
             {subscribe_req_options, SubscribeReqOptions},
             {heartbeat_timeout_window, HeartbeatTimeoutWindow},
             {max_num_resubscribe, MaxNumResubscribe},
             {resubscribe_timeout, ResubscribeTimeout},
             {framework_info, FrameworkInfo},
             {client_ref, ClientRef},
             {subscribe_state, SubscribeState},
             {heartbeat_timeout_ref, HeartbeatTimeoutRef},
             {framework_id, FrameworkId},
             {num_resubscribe, NumResubscribe},
             {resubscribe_timeout_ref, ResubscribeTimeoutRef}],
    [{"Scheduler", Scheduler},
     {"Scheduler state", SchedulerState},
     {"State", State}].
