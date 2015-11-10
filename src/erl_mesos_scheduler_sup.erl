-module(erl_mesos_scheduler_sup).

-export([start_link/0]).

-export([start_scheduler/4, stop_scheduler/1]).

-export([init/1]).

%% External functions.

%% @doc Starts the `erl_mesos_scheduler_sup' process.
-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {}).

%% @doc Starts `erl_mesos_scheduler' process.
-spec start_scheduler(term(), module(), term(),
                      erl_mesos_scheduler:options()) ->
    {ok, pid()} | {error, term()}.
start_scheduler(Ref, Scheduler, SchedulerOptions, Options) ->
    supervisor:start_child(?MODULE, [Ref, Scheduler, SchedulerOptions,
                                     Options]).

%% @doc Stops `erl_mesos_scheduler' process.
-spec stop_scheduler(pid()) -> ok | {error, atom()}.
stop_scheduler(Pid) ->
    supervisor:terminate_child(?MODULE, Pid).

%% supervisor callback function.

%% @private
init({}) ->
    Spec = {undefined,
               {erl_mesos_scheduler, start_link, []},
               temporary, 5000, worker, []},
    {ok, {{simple_one_for_one, 1, 10}, [Spec]}}.
