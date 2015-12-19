%% @private

-module(erl_mesos_sup).

-export([start_link/0]).

-export([init/1]).

%% External functions.

%% @doc Starts the `erl_mesos_sup' process.
-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, {}).

%% supervisor callback function.

%% @private
init({}) ->
    Specs = [{erl_mesos_scheduler_sup,
                  {erl_mesos_scheduler_sup, start_link, []},
                  permanent, 5000, supervisor, [erl_mesos_scheduler_sup]},
             {erl_mesos_scheduler_manager,
                  {erl_mesos_scheduler_manager, start_link, []},
                  permanent, 5000, worker, [erl_mesos_scheduler_manager]}],
    ets:new(erl_mesos_schedulers, [ordered_set, public, named_table]),
    {ok, {{one_for_one, 10, 10}, Specs}}.
