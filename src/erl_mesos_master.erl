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

-module(erl_mesos_master).

-behaviour(gen_server).

-include("master_info.hrl").

-include("master_protobuf.hrl").

-export([start_link/4]).

-export([get_version/1,
         get_state/1,
         get_agents/1,
         get_frameworks/1,
         get_executors/1,
         get_tasks/1,
         get_roles/1,
         get_master/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         format_status/2]).

-record(state, {ref :: term(),
                master :: module(),
                data_format :: erl_mesos_data_format:data_format(),
                data_format_module :: module(),
                api_version :: erl_mesos_master_call:version(),
                master_hosts :: [binary()],
                request_options :: erl_mesos_http:options(),
                max_num_resubscribe :: non_neg_integer(),
                resubscribe_interval :: non_neg_integer(),
                call_subscribe :: undefined | 'Event.Subscribed'(),
                master_state :: undefined | term(),
                master_hosts_queue :: undefined | [binary()],
                master_host :: undefined | binary(),
                client_ref :: undefined | erl_mesos_http:client_ref(),
                recv_timer_ref :: undefined | reference(),
                subscribe_state :: undefined | subscribe_state(),
                num_redirect = 0 :: non_neg_integer(),
                num_resubscribe = 0 :: non_neg_integer(),
                resubscribe_timer_ref :: undefined | reference()}).

-record(subscribe_response, {status :: undefined | non_neg_integer(),
                             headers :: undefined | erl_mesos_http:headers()}).

-type options() :: [{atom(), term()}].
-export_type([options/0]).

-type 'Call'() :: #'Call'{}.
-export_type(['Call'/0]).

-type 'Call.GetMetrics'() :: #'Call.GetMetrics'{}.
-export_type(['Call.GetMetrics'/0]).

-type 'Call.SetLoggingLevel'() :: #'Call.SetLoggingLevel'{}.
-export_type(['Call.SetLoggingLevel'/0]).

-type 'Call.ListFiles'() :: #'Call.ListFiles'{}.
-export_type(['Call.ListFiles'/0]).

-type 'Call.ReadFile'() :: #'Call.ReadFile'{}.
-export_type(['Call.ReadFile'/0]).

-type 'Call.UpdateWeights'() :: #'Call.UpdateWeights'{}.
-export_type(['Call.UpdateWeights'/0]).

-type 'Call.ReserveResources'() :: #'Call.ReserveResources'{}.
-export_type(['Call.ReserveResources'/0]).

-type 'Call.UnreserveResources'() :: #'Call.UnreserveResources'{}.
-export_type(['Call.UnreserveResources'/0]).

-type 'Call.CreateVolumes'() :: #'Call.CreateVolumes'{}.
-export_type(['Call.CreateVolumes'/0]).

-type 'Call.DestroyVolumes'() :: #'Call.DestroyVolumes'{}.
-export_type(['Call.DestroyVolumes'/0]).

-type 'Call.UpdateMaintenanceSchedule'() :: #'Call.UpdateMaintenanceSchedule'{}.
-export_type(['Call.UpdateMaintenanceSchedule'/0]).

-type 'Call.StartMaintenance'() :: #'Call.StartMaintenance'{}.
-export_type(['Call.StartMaintenance'/0]).

-type 'Call.StopMaintenance'() :: #'Call.StopMaintenance'{}.
-export_type(['Call.StopMaintenance'/0]).

-type 'Call.SetQuota'() :: #'Call.SetQuota'{}.
-export_type(['Call.SetQuota'/0]).

-type 'Call.RemoveQuota'() :: #'Call.RemoveQuota'{}.
-export_type(['Call.RemoveQuota'/0]).

-type 'Event'() :: #'Event'{}.
-export_type(['Event'/0]).

-type 'Event.TaskAdded'() :: #'Event.TaskAdded'{}.
-export_type(['Event.TaskAdded'/0]).

-type 'Event.TaskUpdated'() :: #'Event.TaskUpdated'{}.
-export_type(['Event.TaskUpdated'/0]).

-type 'Event.Subscribed'() :: #'Event.Subscribed'{}.
-export_type(['Event.Subscribed'/0]).

-type master_info() :: #master_info{}.
-export_type([master_info/0]).

-type state() :: #state{}.

-type subscribe_response() :: subscribe_response().

-type subscribe_state() :: subscribe_response() | subscribed.

%% Callbacks.

-callback init(term()) ->
    {ok, term()} | {stop, term()}.

-callback subscribed(master_info(), 'Event.Subscribed'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback disconnected(master_info(), term()) ->
    {ok, term()} | {stop, term()}.

-callback task_added(master_info(), 'Event.TaskAdded'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback task_updated(master_info(), 'Event.TaskUpdated'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback handle_info(master_info(), term(), term()) ->
    {ok, term()} | {stop, term()}.

-callback terminate(master_info(), term(), term()) -> term().

-define(DEFAULT_MASTER_HOSTS, [<<"localhost:5050">>]).

-define(DEFAULT_REQUEST_OPTIONS, []).

-define(DEFAULT_RECV_TIMEOUT, 5000).

-define(DEFAULT_MAX_REDIRECT, 5).

-define(DEFAULT_MAX_NUM_RESUBSCRIBE, 1).

-define(DEFAULT_RESUBSCRIBE_INTERVAL, 0).

-define(DATA_FORMAT, protobuf).

-define(DATA_FORMAT_MODULE, master_protobuf).

-define(API_VERSION, v1).

%% External functions.

%% @doc Starts the `erl_mesos_master' process.
-spec start_link(term(), module(), term(), options()) ->
    {ok, pid()} | {error, term()}.
start_link(Ref, Master, MasterOptions, Options) ->
    gen_server:start_link(?MODULE, {Ref, Master, MasterOptions, Options},
                          []).

%% @doc GetVersion call.
-spec get_version(master_info()) -> term() | {error, term()}.
get_version(MasterInfo) ->
    erl_mesos_master_call:get_version(MasterInfo).

%% @doc GetState call.
-spec get_state(master_info()) -> term() | {error, term()}.
get_state(MasterInfo) ->
    erl_mesos_master_call:get_state(MasterInfo).

%% @doc GetAgents call.
-spec get_agents(master_info()) -> term() | {error, term()}.
get_agents(MasterInfo) ->
    erl_mesos_master_call:get_agents(MasterInfo).

%% @doc GetFrameworks call.
-spec get_frameworks(master_info()) -> term() | {error, term()}.
get_frameworks(MasterInfo) ->
    erl_mesos_master_call:get_frameworks(MasterInfo).

%% @doc GetExecutors call.
-spec get_executors(master_info()) -> term() | {error, term()}.
get_executors(MasterInfo) ->
    erl_mesos_master_call:get_executors(MasterInfo).

%% @doc GetTasks call.
-spec get_tasks(master_info()) -> term() | {error, term()}.
get_tasks(MasterInfo) ->
    erl_mesos_master_call:get_tasks(MasterInfo).

%% @doc GetRoles call.
-spec get_roles(master_info()) -> term() | {error, term()}.
get_roles(MasterInfo) ->
    erl_mesos_master_call:get_roles(MasterInfo).

%% @doc GetMaster call.
-spec get_master(master_info()) -> term() | {error, term()}.
get_master(MasterInfo) ->
    erl_mesos_master_call:get_master(MasterInfo).

%% GET_HEALTH
%% GET_FLAGS
%% GET_METRICS
%% GET_LOGGING_LEVEL
%% SET_LOGGING_LEVEL
%% LIST_FILES
%% READ_FILE
%% GET_WEIGHTS
%% UPDATE_WEIGHTS
%% SUBSCRIBE
%% RESERVE_RESOURCES
%% UNRESERVE_RESOURCES
%% CREATE_VOLUMES
%% DESTROY_VOLUMES
%% GET_MAINTENANCE_STATUS
%% GET_MAINTENANCE_SCHEDULE
%% UPDATE_MAINTENANCE_SCHEDULE
%% START_MAINTENANCE
%% STOP_MAINTENANCE
%% GET_QUOTA
%% SET_QUOTA
%% REMOVE_QUOTA

%% gen_server callback functions.

%% @private
-spec init({term(), module(), term(), options()}) ->
    {ok, state()} | {stop, term()}.
init({Ref, Master, MasterOptions, Options}) ->
    case init(Ref, Master, MasterOptions, Options) of
        {ok, State} ->
            {ok, State};
        {error, Reason} ->
            {stop, Reason}
    end.

%% @private
-spec handle_call(term(), {pid(), term()}, state()) -> {noreply, state()}.
handle_call(Request, _From, State) ->
    log_warning("Master received unexpected call request.", "Request: ~p.",
                [Request], State),
    {noreply, State}.

%% @private
-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(Request, State) ->
    log_warning("Master received unexpected cast request.", "Request: ~p.",
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
                         master = Master,
                         master_state = MasterState} = State) ->
    close(ClientRef),
    MasterInfo = master_info(State),
    Master:terminate(MasterInfo, Reason, MasterState).

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
init(Ref, Master, MasterOptions, Options) ->
    Funs = [fun master_hosts/1,
            fun request_options/1,
            fun max_num_resubscribe/1,
            fun resubscribe_interval/1],
    case options(Funs, Options) of
        {ok, ValidOptions} ->
            State = state(Ref, Master, ValidOptions),
            case init(MasterOptions, State) of
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
state(Ref, Master, Options) ->
    MasterHosts = proplists:get_value(master_hosts, Options),
    RequestOptions = proplists:get_value(request_options, Options),
    MaxNumResubscribe = proplists:get_value(max_num_resubscribe, Options),
    ResubscribeInterval = proplists:get_value(resubscribe_interval, Options),
    #state{ref = Ref,
           master = Master,
           data_format = ?DATA_FORMAT,
           data_format_module = ?DATA_FORMAT_MODULE,
           api_version = ?API_VERSION,
           master_hosts = MasterHosts,
           request_options = RequestOptions,
           max_num_resubscribe = MaxNumResubscribe,
           resubscribe_interval = ResubscribeInterval}.

%% @doc Calls Master:init/1.
%% @private
-spec init(term(), state()) -> {ok, state()} | {stop, term()}.
init(MasterOptions, #state{master_hosts = MasterHosts,
                              master = Master} = State) ->
    case Master:init(MasterOptions) of
        {ok, MasterState} ->
            {ok, State#state{master_state = MasterState,
                             master_hosts_queue = MasterHosts}};
        {stop, Reason} ->
            {stop, Reason}
    end.

%% @doc Sends subscribe requests.
%% @private
-spec subscribe(state()) -> {ok, state()} | {error, bad_hosts}.
subscribe(#state{master_hosts_queue = [MasterHost | MasterHostsQueue]} =
          State) ->
    log_info("Try to subscribe.", "Host: ~s.", [MasterHost], State),
    MasterInfo = master_info(State),
    MasterInfo1 = MasterInfo#master_info{master_host = MasterHost},
    case erl_mesos_master_call:subscribe(MasterInfo1) of
        {ok, ClientRef} ->
            State1 = State#state{master_hosts_queue = MasterHostsQueue,
                                 master_host = MasterHost,
                                 client_ref = ClientRef},
            State2 = set_recv_timer(State1),
            {ok, State2};
        {error, Reason} ->
            log_error("Can not subscribe.", "Host: ~s, Error reason ~p.",
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

%% @doc Returns master info.
%% @private
-spec master_info(state()) -> master_info().
master_info(#state{data_format = DataFormat,
                   data_format_module = DataFormatModule,
                   api_version = ApiVersion,
                   master_host = MasterHost,
                   request_options = RequestOptions,
                   subscribe_state = SubscribeState}) ->
    Subscribed = SubscribeState =:= subscribed,
    #master_info{data_format = DataFormat,
                 data_format_module = DataFormatModule,
                 api_version = ApiVersion,
                 master_host = MasterHost,
                 request_options = RequestOptions,
                 subscribed = Subscribed}.

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
handle_async_response(_Body,
                      #state{recv_timer_ref = RecvTimerRef,
                             subscribe_state =
                             #subscribe_response{status = 307}} = State) ->
    cancel_recv_timer(RecvTimerRef),
    handle_redirect(State);
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
apply_event(Message, #state{master_host = MasterHost,
                            subscribe_state = SubscribeState} = State) ->
    case Message of
        #'Event'{type = 'SUBSCRIBED',
                 subscribed = EventSubscribed}
          when is_record(SubscribeState, subscribe_response) ->
            log_info("Successfully subscribed.", "Host: ~s.", [MasterHost],
                     State),
            {EventSubscribed1, State1} = set_subscribed(EventSubscribed, State),
            call(subscribed, EventSubscribed1, State1);
        #'Event'{type = 'TASK_ADDED', task_added = TaskAdded} ->
            call(task_added, TaskAdded, State);
        #'Event'{type = 'TASK_UPDATED', task_updated = TaskUpdated} ->
            call(offer_rescinded, TaskUpdated, State)
    end.

%% @doc Sets subscribed state.
%% @private
-spec set_subscribed('Event.Subscribed'(), state()) ->
    {'Event.Subscribed'(), state()}.
set_subscribed(EventSubscribed,
               #state{call_subscribe = CallSubscribe} =
               State) ->
    State1 = State#state{call_subscribe = CallSubscribe,
                         master_hosts_queue = undefined,
                         subscribe_state = subscribed,
                         num_redirect = 0,
                         num_resubscribe = 0},
    {EventSubscribed, State1}.

%% @doc Calls Master:Callback/2.
%% @private
-spec call(atom(), state()) -> {ok, state()} | {stop, state()}.
call(Callback, #state{master = Master,
                      master_state = MasterState} = State) ->
    MasterInfo = master_info(State),
    case Master:Callback(MasterInfo, MasterState) of
        {ok, MasterState1} ->
            {ok, State#state{master_state = MasterState1}};
        {stop, MasterState1} ->
            {stop, State#state{master_state = MasterState1}}
    end.

%% @doc Calls Master:Callback/3.
%% @private
-spec call(atom(), term(), state()) -> {ok, state()} | {stop, state()}.
call(Callback, Arg, #state{master = Master,
                           master_state = MasterState} = State) ->
    MasterInfo = master_info(State),
    case Master:Callback(MasterInfo, Arg, MasterState) of
        {ok, MasterState1} ->
            {ok, State#state{master_state = MasterState1}};
        {stop, MasterState1} ->
            {stop, State#state{master_state = MasterState1}}
    end.

%% @doc Handles unsubscribe.
%% @private
-spec handle_unsubscribe(state()) ->
    {noreply, state()} | {stop, term(), state()}.
%% handle_unsubscribe(#state{client_ref = ClientRef,
%%                           recv_timer_ref = RecvTimerRef} = State) ->
%%     case subscribe(State) of
%%         {ok, State1} ->
%%             close(ClientRef),
%%             cancel_recv_timer(RecvTimerRef),
%%             State2 = State1#state{client_ref = undefined,
%%                                   subscribe_state = undefined},
%%             {noreply, State2};
%%         {error, Reason} ->
%%             {stop, {shutdown, {subscribe, {error, Reason}}}, State}
%%     end;
handle_unsubscribe(#state{recv_timer_ref = RecvTimerRef,
                          subscribe_state = subscribed} = State) ->
    State1 = State#state{subscribe_state = undefined},
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
    State1 = State#state{subscribe_state = undefined},
    start_resubscribe_timer(State1).

%% @doc Start resubscribe timer.
%% @private
-spec start_resubscribe_timer(state()) ->
    {noreply, state()} | {stop, term(), state()}.
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
resubscribe(#state{master_host = MasterHost} = State) ->
    log_info("Try to resubscribe.", "Host: ~s.", [MasterHost], State),
    MasterInfo = master_info(State),
    case erl_mesos_master_call:subscribe(MasterInfo) of
        {ok, ClientRef} ->
            State1 = State#state{client_ref = ClientRef},
            State2 = set_recv_timer(State1),
            {noreply, State2};
        {error, Reason} ->
            log_error("Can not resubscribe.", "Host: ~s, Error reason: ~p.",
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
                       client_ref = ClientRef,
                       subscribe_state =
                           #subscribe_response{headers = Headers},
                       num_redirect = NumRedirect} = State) ->
    case proplists:get_value(max_redirect, RequestOptions,
                             ?DEFAULT_MAX_REDIRECT) of
        NumRedirect ->
            {stop, {shutdown, {resubscribe, {error, max_redirect}}}, State};
        _MaxNumRedirect ->
            close(ClientRef),
            MasterHost1 = redirect_master_host(Headers),
            log_info("Redirect.", "Form host: ~s, to host: ~s.",
                     [MasterHost, MasterHost1], State),
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
%% redirect(State) ->
%%     case subscribe(State) of
%%         {ok, State1} ->
%%             {noreply, State1};
%%         {error, Reason} ->
%%             {stop, {shutdown, {subscribe, {error, Reason}}}, State}
%%     end;
redirect(#state{master_hosts_queue = [MasterHost | MasterHostsQueue]} =
         State) ->
    State1 = State#state{master_hosts_queue = MasterHostsQueue,
                         master_host = MasterHost,
                         num_resubscribe = 0},
    resubscribe(State1).

%% @doc Calls Master:handle_info/3.
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
-spec log_info(string(), string(), [term()], state()) -> ok.
log_info(Message, Format, Data, #state{ref = Ref, master = Master}) ->
    erl_mesos_logger:info(Message ++ " Ref: ~p, Master: ~p, " ++ Format,
                          [Ref, Master | Data]).

%% @doc Logs warning.
%% @private
-spec log_warning(string(), string(), [term()], state()) -> ok.
log_warning(Message, Format, Data, #state{ref = Ref, master = Master}) ->
    erl_mesos_logger:warning(Message ++ " Ref: ~p, Master: ~p, " ++ Format,
                             [Ref, Master | Data]).

%% @doc Logs error.
%% @private
-spec log_error(string(), state()) -> ok.
log_error(Message, #state{ref = Ref, master = Master}) ->
    erl_mesos_logger:error(Message ++ " Ref: ~p, Master: ~p.",
                           [Ref, Master]).

%% @doc Logs error.
%% @private
-spec log_error(string(), string(), [term()], state()) -> ok.
log_error(Message, Format, Data, #state{ref = Ref, master = Master}) ->
    erl_mesos_logger:error(Message ++ " Ref: ~p, Master: ~p, " ++ Format,
                           [Ref, Master | Data]).

%% @doc Formats state.
%% @private
-spec format_state(state()) -> [{string(), [{atom(), term()}]}].
format_state(#state{ref = Ref,
                    master = Master,
                    data_format = DataFormat,
                    data_format_module = DataFormatModule,
                    api_version = ApiVersion,
                    master_hosts = MasterHosts,
                    request_options = RequestOptions,
                    max_num_resubscribe = MaxNumResubscribe,
                    resubscribe_interval = ResubscribeInterval,
                    call_subscribe = CallSubscribe,
                    master_state = MasterState,
                    master_hosts_queue = MasterHostsQueue,
                    master_host = MasterHost,
                    client_ref = ClientRef,
                    recv_timer_ref = RecvTimerRef,
                    subscribe_state = SubscribeState,
                    num_redirect = NumRedirect,
                    num_resubscribe = NumResubscribe,
                    resubscribe_timer_ref = ResubscribeTimerRef}) ->
    State = [{data_format, DataFormat},
             {data_format_module, DataFormatModule},
             {api_version, ApiVersion},
             {master_hosts, MasterHosts},
             {request_options, RequestOptions},
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
             {resubscribe_timer_ref, ResubscribeTimerRef}],
    [{"Ref", Ref},
     {"Master", Master},
     {"Master state", MasterState},
     {"State", State}].
