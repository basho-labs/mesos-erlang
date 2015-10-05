%% @private

-module(erl_mesos_sup).

-export([start_link/0]).

-export([start_scheduler/3]).

-export([init/1]).

%% External functions.

%% @doc Starts the `erl_mesos_sup' process.
-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {}).

%% @doc Starts `erl_mesos_scheduler' process.
-spec start_scheduler(module(), term(), erl_mesos_scheduler:options()) ->
    {ok, pid()} | {error, term()}.
start_scheduler(Scheduler, SchedulerOptions, Options) ->
    supervisor:start_child(?MODULE, [Scheduler, SchedulerOptions, Options]).

%% supervisor callback function.

%% @private
init({}) ->
    Spec = {erl_mesos_scheduler,
               {erl_mesos_scheduler, start_link, []},
               temporary, 5000, worker, [erl_mesos_scheduler]},
    {ok, {{simple_one_for_one, 1, 10}, [Spec]}}.
