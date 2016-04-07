# Erlang Mesos library

## Scheduler api

### Creating scheduler

`erl_mesos_scheduler` process a wrapper on top of `gen_server` process.
Each scheduler handler must implement `erl_mesos_scheduler` behaviour.

`erl_mesos_scheduler` callbacks:

#### Module:init/1

```erlang
Module:init(Options) -> 
    {ok, FrameworkInfo, Force, State} | {stop, Reason}.
```

Data types:

```erlang
Options = term()
FrameworkInfo = erl_mesos:'FrameworkInfo'()
Force = boolean()
State = term()
```

Whenever a `erl_mesos_scheduler` process is started using 
`erl_mesos:start_scheduler/4` or `erl_mesos_scheduler:start_link/4` 
this function is called by the new process to initialize the framework.

#### Module:registered/3

```erlang
Module:registered(SchedulerInfo, EventSubscribed, State) ->
    {ok, NewState} | {stop, NewState}
```

Data types:

```erlang
SchedulerInfo = scheduler_info()
EventSubscribed = erl_mesos:'Event.Subscribed'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_scheduler` process receives subscribed event from
Mesos this function is called to handle this event.

#### Module:disconnected/2

```erlang
Module:disconnected(SchedulerInfo, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
SchedulerInfo = scheduler_info()
State = term()
NewState = term()
```

Whenever a `erl_mesos_scheduler` process disconnected from
Mesos this function is called to handle the disconnection. 
Then the `erl_mesos_scheduler` process will try to reconnect to one of 
the Mesos masters. In case of success reconnection the 
`Module:reregistered/2` will be called. Otherwise `erl_mesos_scheduler` 
will be stopped.

#### Module:reregistered/2

```erlang
Module:reregistered(SchedulerInfo, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
SchedulerInfo = scheduler_info()
State = term()
NewState = term()
```

Whenever a `erl_mesos_scheduler` process reregistered to the Mesos 
master this function is called to handle the registration.

#### Module:offer_rescinded/3

```erlang
Module:offer_rescinded(SchedulerInfo, EventRescind, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
SchedulerInfo = scheduler_info()
EventRescind = erl_mesos:'Event.Rescind'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_scheduler` process receives rescind event from
Mesos this function is called to handle this event.

#### Module:status_update/3

```erlang
Module:status_update(SchedulerInfo, EventUpdate, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
SchedulerInfo = scheduler_info()
EventUpdate = erl_mesos:'Event.Update'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_scheduler` process receives update event from
Mesos this function is called to handle this event.

#### Module:framework_message/3

```erlang
Module:framework_message(SchedulerInfo, EventMessage, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
SchedulerInfo = scheduler_info()
EventMessage = erl_mesos:'EventMessage'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_scheduler` process receives message event from
Mesos this function is called to handle this event.


TODO: finish callbacks.
-callback slave_lost(scheduler_info(), erl_mesos:'Event.Failure'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback executor_lost(scheduler_info(), erl_mesos:'Event.Failure'(),
                        term()) ->
    {ok, term()} | {stop, term()}.

-callback error(scheduler_info(), erl_mesos:'Event.Error'(), term()) ->
    {ok, term()} | {stop, term()}.

-callback handle_info(scheduler_info(), term(), term()) ->
    {ok, term()} | {stop, term()}.

-callback terminate(scheduler_info(), term(), term()) -> term().

