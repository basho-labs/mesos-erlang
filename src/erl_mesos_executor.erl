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

-module(erl_mesos_executor).

-behaviour(gen_server).

-include("executor_info.hrl").

-include("executor_protobuf.hrl").

-export([start_link/4]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         format_status/2]).

-record(state, {ref :: term(),
                executor :: module(),
                data_format :: erl_mesos_data_format:data_format(),
                data_format_module :: module(),
                api_version :: erl_mesos_executor_call:version(),
                request_options :: erl_mesos_http:options(),
                agent_host :: binary(),
                executor_id :: erl_mesos:'ExecutorID'(),
                framework_id :: erl_mesos:'FrameworkID'(),
                max_num_resubscribe :: non_neg_integer(),
                resubscribe_interval :: non_neg_integer(),
                registered = false :: boolean(),
                call_subscribe :: undefined | 'Call.Subscribe'(),
                executor_state :: term(),
                client_ref :: undefined | erl_mesos_http:client_ref(),
                recv_timer_ref :: undefined | reference(),
                subscribe_state :: undefined | subscribe_state(),
                num_resubscribe = 0 :: non_neg_integer(),
                resubscribe_timer_ref :: undefined | reference()}).

-record(subscribe_response, {status :: undefined | non_neg_integer(),
                             headers :: undefined | erl_mesos_http:headers()}).

-type options() :: [{atom(), term()}].
-export_type([options/0]).

-type 'Call'() :: #'Call'{}.
-export_type(['Call'/0]).

-type 'Call.Subscribe'() :: #'Call.Subscribe'{}.
-export_type(['Call.Subscribe'/0]).

-type 'Call.Update'() :: #'Call.Update'{}.
-export_type(['Call.Update'/0]).

-type 'Event'() :: #'Event'{}.
-export_type(['Event'/0]).

-type 'Event.Subscribed'() :: #'Event.Subscribed'{}.
-export_type(['Event.Subscribed'/0]).

-type executor_info() :: #executor_info{}.
-export_type([executor_info/0]).

-type state() :: #state{}.

-type subscribe_response() :: subscribe_response().

-type subscribe_state() :: subscribe_response() | subscribed.

%% Callbacks.

-callback init(term()) ->
    {ok, 'Call.Subscribe'(), term()} |
    {stop, term()}.

-callback registered(executor_info(), 'Event.Subscribed'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback disconnected(executor_info(), term()) ->
    {ok, 'Call.Subscribe'(), term()} | {stop, term()}.

-callback reregistered(executor_info(), term()) ->
    {ok, term()} | {stop, term()}.

-define(DEFAULT_REQUEST_OPTIONS, []).

-define(DEFAULT_RECV_TIMEOUT, 5000).

-define(DATA_FORMAT, protobuf).

-define(DATA_FORMAT_MODULE, executor_protobuf).

-define(API_VERSION, v1).

%% External functions.

%% @doc Starts the `erl_mesos_executor' process.
-spec start_link(term(), module(), term(), options()) ->
    {ok, pid()} | {error, term()}.
start_link(Ref, Executor, ExecutorOptions, Options) ->
    gen_server:start_link(?MODULE, {Ref, Executor, ExecutorOptions, Options},
                          []).

%% gen_server callback functions.

%% @private
-spec init({term(), module(), term(), options()}) ->
    {ok, state()} | {stop, term()}.
init({Ref, Executor, ExecutorOptions, Options}) ->
    case init(Ref, Executor, ExecutorOptions, Options) of
        {ok, State} ->
            {ok, State};
        {error, Reason} ->
            {stop, Reason}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, state()) -> {noreply, state()}.
handle_call(Request, _From, State) ->
    log_warning("Executor received unexpected call request.", "Request: ~p.",
                [Request], State),
    {noreply, State}.

%% @private
-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Request, State) ->
    log_warning("Executor received unexpected cast request.", "Request: ~p.",
                [Request], State),
    {noreply, State}.

%% @private
-spec handle_info(term(), state()) ->
    {noreply, state()} | {stop, term(), state()}.
handle_info(Info, #state{client_ref = ClientRef,
                         recv_timer_ref = RecvTimerRef,
                         subscribe_state = SubscribeState,
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
                    log_error("Client process crashed.", "Reason: ~p.",
                              [Reason], State),
                    handle_unsubscribe(State);
                {timeout, RecvTimerRef, recv}
                  when is_reference(RecvTimerRef),
                    SubscribeState =/= subscribed ->
                    log_error("Receive timeout occurred.", State),
                    handle_unsubscribe(State);
                {timeout, RecvTimerRef, recv}
                  when is_reference(RecvTimerRef) ->
                    {noreply, State};
                {timeout, ResubscribeTimerRef, resubscribe}
                  when SubscribeState =:= undefined ->
                    %%resubscribe(State);
                    {noreply, State};
                {timeout, ResubscribeTimerRef, resubscribe} ->
                    {noreply, State};
                _Info ->
                    %% call_handle_info(Info, State)
                    {noreply, State}
            end
    end.

%% @private
-spec terminate(term(), state()) -> term().
terminate(_Reason, _State) ->
    %% TODO: implement terminate call.
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

%% @doc Validates options and sets options to the state.
%% @private
-spec init(term(), module(), term(), options()) ->
    {ok, state()} | {error, term()}.
init(Ref, Executor, ExecutorOptions, Options) ->
    Funs = [fun request_options/1],
    case options(Funs, Options) of
        {ok, ValidOptions} ->
            State = state(Ref, Executor, ValidOptions),
            case init(ExecutorOptions, State) of
                {ok, State1} ->
                    subscribe(State1);
                {stop, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

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
state(Ref, Executor, Options) ->
    RequestOptions = proplists:get_value(request_options, Options),
    AgentHost = erl_mesos_env:get_converted_value(binary, agent_endpoint),
    ExecutorIdValue = erl_mesos_env:get_converted_value(string, executor_id),
    FrameworkIdValue = erl_mesos_env:get_converted_value(string, framework_id),
    ExecutorId = #'ExecutorID'{value = ExecutorIdValue},
    FrameworkId = #'FrameworkID'{value = FrameworkIdValue},
    RecoveryTimeout = erl_mesos_env:get_converted_value(interval,
                                                        recovery_timeout),
    SubscriptionBackoffMax =
        erl_mesos_env:get_converted_value(interval, subscription_backoff_max),
    {MaxNumResubscribe, ResubscribeInterval} =
        resubscribe(RecoveryTimeout, SubscriptionBackoffMax),
    #state{ref = Ref,
           executor = Executor,
           data_format = ?DATA_FORMAT,
           data_format_module = ?DATA_FORMAT_MODULE,
           api_version = ?API_VERSION,
           request_options = RequestOptions,
           agent_host = AgentHost,
           executor_id = ExecutorId,
           framework_id = FrameworkId,
           max_num_resubscribe = MaxNumResubscribe,
           resubscribe_interval = ResubscribeInterval}.

%% @doc Returns resubscribe.
%% @private
-spec resubscribe(undefined | float(), undefined | float()) ->
    {non_neg_integer(), non_neg_integer()}.
resubscribe(undefined, _SubscriptionBackoffMax) ->
    {0, 0};
resubscribe(_RecoveryTimeout, undefined) ->
    {0, 0};
resubscribe(RecoveryTimeout, SubscriptionBackoffMax) ->
    case trunc(SubscriptionBackoffMax) of
        0 ->
            {0, 0};
        ResubscribeInterval ->
            MaxNumResubscribe = trunc(RecoveryTimeout / ResubscribeInterval),
            {MaxNumResubscribe, ResubscribeInterval}
    end.

%% @doc Calls Executor:init/1.
%% @private
-spec init(term(), state()) -> {ok, state()} | {stop, term()}.
init(ExecutorOptions, #state{executor = Executor} = State) ->
    case Executor:init(ExecutorOptions) of
        {ok, CallSubscribe, ExecutorState}
          when is_record(CallSubscribe, 'Call.Subscribe') ->
            {ok, State#state{call_subscribe = CallSubscribe,
                             executor_state = ExecutorState}};
        {stop, Reason} ->
            {stop, Reason}
    end.

%% @doc Sends subscribe requests.
%% @private
-spec subscribe(state()) -> {ok, state()} | {error, term()}.
subscribe(#state{agent_host = AgentHost,
                 call_subscribe = CallSubscribe} = State) ->
    log_info("Try to subscribe.", "Host: ~s.", [AgentHost], State),
    ExecutorInfo = executor_info(State),
    case erl_mesos_executor_call:subscribe(ExecutorInfo, CallSubscribe) of
        {ok, ClientRef} ->
            State1 = State#state{client_ref = ClientRef},
            State2 = set_recv_timer(State1),
            {ok, State2};
        {error, Reason} ->
            log_error("Can not subscribe.", "Host: ~s, Error reason ~p.",
                      [AgentHost, Reason], State),
            {error, Reason}
    end.

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

%% @doc Returns executor info.
%% @private
-spec executor_info(state()) -> executor_info().
executor_info(#state{data_format = DataFormat,
                     data_format_module = DataFormatModule,
                     api_version = ApiVersion,
                     request_options = RequestOptions,
                     agent_host = AgentHost,
                     executor_id = ExecutorId,
                     framework_id = FrameworkId,
                     subscribe_state = SubscribeState}) ->
    Subscribed = SubscribeState =:= subscribed,
    #executor_info{data_format = DataFormat,
                   data_format_module = DataFormatModule,
                   api_version = ApiVersion,
                   request_options = RequestOptions,
                   agent_host = AgentHost,
                   subscribed = Subscribed,
                   executor_id = ExecutorId,
                   framework_id = FrameworkId}.

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
            log_error("Invalid content type.", "Content type: ~s.",
                      [ContentType], State),
            handle_unsubscribe(State)
    end;
handle_async_response(Events, #state{subscribe_state = subscribed} = State)
  when is_binary(Events) ->
    handle_events(Events, State);
handle_async_response(Body,
                      #state{subscribe_state =
                             #subscribe_response{status = Status}} = State) ->
    log_error("Invalid http response.", "Status: ~p, Body: ~s.", [Status, Body],
              State),
    handle_unsubscribe(State);
handle_async_response(done, State) ->
    log_error("Connection closed.", State),
    handle_unsubscribe(State);
handle_async_response({error, Reason}, State) ->
    log_error("Connection error.", "Reason: ~p.", [Reason], State),
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
apply_event(Message, #state{agent_host = AgentHost,
                            registered = Registered,
                            subscribe_state = SubscribeState} = State) ->
    case Message of
        #'Event'{type = 'SUBSCRIBED',
                 subscribed = EventSubscribed}
          when is_record(SubscribeState, subscribe_response), not Registered ->
            log_info("Successfully subscribed.", "Host: ~s.", [AgentHost],
                     State),
            State1 = set_subscribed(State),
            call(registered, EventSubscribed, State1#state{registered = true});
        #'Event'{type = 'SUBSCRIBED'}
          when is_record(SubscribeState, subscribe_response) ->
            log_info("Successfully resubscribed.", "Host: ~s.", [AgentHost],
                     State),
            State1 = set_subscribed(State),
            call(reregistered, State1);
        Event ->
            io:format("Event: ~p~n", [Event]),
            {ok, State}
    end.

%% @doc Sets subscribed state.
%% @private
-spec set_subscribed(state()) -> state().
set_subscribed(State) ->
    State#state{subscribe_state = subscribed, num_resubscribe = 0}.

%% @doc Calls Executor:Callback/2.
%% @private
-spec call(atom(), state()) -> {ok, state()} | {stop, state()}.
call(Callback, #state{executor = Executor,
                      executor_state = ExecutorState} = State) ->
    ExecutorInfo = executor_info(State),
    case Executor:Callback(ExecutorInfo, ExecutorState) of
        {ok, ExecutorState1} ->
            {ok, State#state{executor_state = ExecutorState1}};
        {stop, ExecutorState1} ->
            {stop, State#state{executor_state = ExecutorState1}}
    end.

%% @doc Calls Scheduler:Callback/3.
%% @private
-spec call(atom(), term(), state()) -> {ok, state()} | {stop, state()}.
call(Callback, Arg, #state{executor = Executor,
                           executor_state = ExecutorState} = State) ->
    ExecutorInfo = executor_info(State),
    case Executor:Callback(ExecutorInfo, Arg, ExecutorState) of
        {ok, ExecutorState1} ->
            {ok, State#state{executor_state = ExecutorState1}};
        {stop, ExecutorState1} ->
            {stop, State#state{executor_state = ExecutorState1}}
    end.

handle_unsubscribe(State) ->
    %% TODO: implement unsubscribe.
    io:format("handle_unsubscribe"),
    {stop, normal, State}.

%% @doc Logs info.
%% @private
-spec log_info(string(), string(), [term()], state()) -> ok.
log_info(Message, Format, Data, #state{ref = Ref, executor = Executor}) ->
    erl_mesos_logger:info(Message ++ " Ref: ~p, Executor: ~p, " ++ Format,
                          [Ref, Executor | Data]).

%% @doc Logs warning.
%% @private
-spec log_warning(string(), string(), [term()], state()) -> ok.
log_warning(Message, Format, Data, #state{ref = Ref, executor = Executor}) ->
    erl_mesos_logger:warning(Message ++ " Ref: ~p, Executor: ~p, " ++ Format,
                             [Ref, Executor | Data]).

%% @doc Logs error.
%% @private
-spec log_error(string(), state()) -> ok.
log_error(Message, #state{ref = Ref, executor = Executor}) ->
    erl_mesos_logger:error(Message ++ " Ref: ~p, Executor: ~p.",
                           [Ref, Executor]).

%% @doc Logs error.
%% @private
-spec log_error(string(), string(), [term()], state()) -> ok.
log_error(Message, Format, Data, #state{ref = Ref, executor = Executor}) ->
    erl_mesos_logger:error(Message ++ " Ref: ~p, Executor: ~p, " ++ Format,
                           [Ref, Executor | Data]).

%% @doc Formats state.
%% @private
-spec format_state(state()) -> [{string(), [{atom(), term()}]}].
format_state(#state{ref = Ref,
                    executor = Executor,
                    data_format = DataFormat,
                    data_format_module = DataFormatModule,
                    api_version = ApiVersion,
                    request_options = RequestOptions,
                    agent_host = AgentHost,
                    executor_id = ExecutorId,
                    framework_id = FrameworkId,
                    max_num_resubscribe = MaxNumResubscribe,
                    resubscribe_interval = ResubscribeInterval,
                    registered = Registered,
                    call_subscribe = CallSubscribe,
                    executor_state = ExecutorState,
                    client_ref = ClientRef,
                    recv_timer_ref = RecvTimerRef,
                    subscribe_state = SubscribeState,
                    num_resubscribe = NumResubscribe,
                    resubscribe_timer_ref = ResubscribeTimerRef}) ->
    State = [{data_format, DataFormat},
             {data_format_module, DataFormatModule},
             {api_version, ApiVersion},
             {request_options, RequestOptions},
             {agent_host, AgentHost},
             {executor_id, ExecutorId},
             {framework_id, FrameworkId},
             {max_num_resubscribe, MaxNumResubscribe},
             {resubscribe_interval, ResubscribeInterval},
             {registered, Registered},
             {call_subscribe, CallSubscribe},
             {client_ref, ClientRef},
             {recv_timer_ref, RecvTimerRef},
             {subscribe_state, SubscribeState},
             {num_resubscribe, NumResubscribe},
             {resubscribe_timer_ref, ResubscribeTimerRef}],
    [{"Ref", Ref},
     {"Executor", Executor},
     {"Executor state", ExecutorState},
     {"State", State}].
