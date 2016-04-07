# Erlang Mesos library

## Scheduler

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
EventMessage = erl_mesos:'Event.Message'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_scheduler` process receives message event from
Mesos this function is called to handle this event.

#### Module:slave_lost/3

```erlang
Module:slave_lost(SchedulerInfo, EventFailure, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
SchedulerInfo = scheduler_info()
EventFailure = erl_mesos:'Event.Failure'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_scheduler` process receives failure event without 
executor id data from Mesos this function is called to handle this 
event.

#### Module:executor_lost/3

```erlang
Module:executor_lost(SchedulerInfo, EventFailure, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
SchedulerInfo = scheduler_info()
EventFailure = erl_mesos:'Event.Failure'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_scheduler` process receives failure event with 
executor id data from Mesos this function is called to handle this 
event.

#### Module:error/3

```erlang
Module:error(SchedulerInfo, EventError, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
SchedulerInfo = scheduler_info()
EventError = erl_mesos:'Event.Error'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_scheduler` process receives error event from Mesos 
this function is called to handle this event.

#### Module:handle_info/3

```erlang
Module:handle_info(SchedulerInfo, Info, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
SchedulerInfo = scheduler_info()
EventError = erl_mesos:'Event.Error'()
State = term()
NewState = term()
```

This function is called by a `erl_mesos_scheduler` it receives any other 
message than Mesos event.

#### Module:terminate/3

```erlang
Module:terminate(SchedulerInfo, Reason, State) -> Result.
```

Data types:

```erlang
SchedulerInfo = scheduler_info()
Reason = term()
State = term()
Result = term()
```

This function is called by a `erl_mesos_scheduler` when it is about to 
terminate.
