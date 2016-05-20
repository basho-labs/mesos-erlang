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
                api_version :: erl_mesos_executor_call:version(),
                agent_host :: binary(),
                request_options :: erl_mesos_http:options(),
                registered = false :: boolean(),
                executor_state :: term()}).

%%-record(subscribe_response, {status :: undefined | non_neg_integer(),
%%                             headers :: undefined | erl_mesos_http:headers()}).

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

%%-type subscribe_state() :: subscribe_response() | subscribed.

%% Callbacks.

-callback init(term()) ->
    {ok, 'Call.Subscribe'(), term()} |
    {stop, term()}.

-callback registered(executor_info(), 'Event.Subscribed'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback disconnected(executor_info(), term()) ->
    {ok, term()} | {stop, term()}.

-callback reregistered(executor_info(), term()) ->
    {ok, 'Call.Subscribe'(), term()} | {stop, term()}.

-define(DEFAULT_AGENT_HOST, <<"localhost:5051">>).

-define(DEFAULT_REQUEST_OPTIONS, []).

-define(DEFAULT_RECV_TIMEOUT, 5000).

-define(DATA_FORMAT, protobuf).

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
%%
%% @private
-spec handle_info(term(), state()) ->
    {noreply, state()} | {stop, term(), state()}.
handle_info(_Info, State) ->
    %% TODO: implement handle info call.
    {noreply, State}.

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
    Funs = [fun agent_host/1, fun request_options/1],
    case options(Funs, Options) of
        {ok, ValidOptions} ->
            State = state(Ref, Executor, ValidOptions),
            case init(ExecutorOptions, State) of
                {ok, CallSubscribe, State1} ->
                    subscribe(CallSubscribe, State1);
                {stop, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Returns agent host.
%% @private
-spec agent_host(options()) ->
    {ok, {agent_host, binary()}} | {error, {bad_agent_host, term()}}.
agent_host(Options) ->
    case proplists:get_value(agent_host, Options, ?DEFAULT_AGENT_HOST) of
        AgentHost when is_list(AgentHost) ->
            {ok, {agent_host, list_to_binary(AgentHost)}};
        AgentHost when is_binary(AgentHost) ->
            {ok, {agent_host, AgentHost}};
        AgentHost ->
            {error, {bad_agent_host, AgentHost}}
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
    AgentHost = proplists:get_value(agent_host, Options),
    RequestOptions = proplists:get_value(request_options, Options),
    #state{ref = Ref,
           executor = Executor,
           data_format = ?DATA_FORMAT,
           api_version = ?API_VERSION,
           agent_host = AgentHost,
           request_options = RequestOptions}.

%% @doc Calls Executor:init/1.
%% @private
-spec init(term(), state()) ->
    {ok, 'Call.Subscribe'(), state()} | {stop, term()}.
init(ExecutorOptions, #state{executor = Executor} = State) ->
    case Executor:init(ExecutorOptions) of
        {ok, CallSubscribe, SchedulerState}
          when is_record(CallSubscribe, 'Call.Subscribe') ->
            {ok, CallSubscribe, State#state{executor_state = SchedulerState}};
        {stop, Reason} ->
            {stop, Reason}
    end.

subscribe(_CallSubscribe, State) ->
    %% TODO: implement subscribe call.
    log_info("SUBSCIRBE", "HERE", [], State),
    {ok, State}.

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

%%%% @doc Logs error.
%%%% @private
%%-spec log_error(string(), state()) -> ok.
%%log_error(Message, #state{ref = Ref, executor = Executor}) ->
%%    erl_mesos_logger:error(Message ++ " Ref: ~p, Executor: ~p.",
%%                           [Ref, Executor]).
%%
%%%% @doc Logs error.
%%%% @private
%%-spec log_error(string(), string(), [term()], state()) -> ok.
%%log_error(Message, Format, Data, #state{ref = Ref, executor = Executor}) ->
%%    erl_mesos_logger:error(Message ++ " Ref: ~p, Executor: ~p, " ++ Format,
%%                           [Ref, Executor | Data]).

%% @doc Formats state.
%% @private
-spec format_state(state()) -> [{string(), [{atom(), term()}]}].
format_state(#state{ref = Ref,
                    executor = Executor,
                    data_format = DataFormat,
                    api_version = ApiVersion,
                    agent_host = AgentHost,
                    request_options = RequestOptions,
                    registered = Registered,
                    executor_state = ExecutorState}) ->
    State = [{data_format, DataFormat},
             {api_version, ApiVersion},
             {agent_host, AgentHost},
             {request_options, RequestOptions},
             {registered, Registered}],
    [{"Ref", Ref},
     {"Executor", Executor},
     {"Executor state", ExecutorState},
     {"State", State}].
