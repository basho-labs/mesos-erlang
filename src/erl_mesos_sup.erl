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
                  permanent, 5000, supervisor, [erl_mesos_scheduler_sup]}],
    {ok, {{one_for_one, 10, 10}, Specs}}.
