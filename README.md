# Erlang Mesos library

## Scheduler

`erl_mesos_scheduler` process a wrapper on top of `gen_server` process.

### Scheduler callbacks

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
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
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
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
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
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
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
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
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
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
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
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
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
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
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
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
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
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
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
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
EventError = erl_mesos:'Event.Error'()
State = term()
NewState = term()
```

This function is called by a `erl_mesos_scheduler` it receives any other 
message than Mesos event.

#### Module:terminate/3

```erlang
Module:terminate(SchedulerInfo, Reason, State) -> 
    Result.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
Reason = term()
State = term()
Result = term()
```

This function is called by a `erl_mesos_scheduler` when it is about to 
terminate.

### Scheduler calls

#### Teardown

```erlang
erl_mesos_scheduler:teardown(SchedulerInfo) -> 
    ok | {error, Reason}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
Reason = term()
```

Teardown call.

#### Accept

```erlang
erl_mesos_scheduler:accept(SchedulerInfo, OfferIds, Operations) -> 
    ok | {error, Reason}.
```

```erlang
erl_mesos_scheduler:accept(SchedulerInfo, OfferIds, Operations, 
                           Filters) -> 
    ok | {error, Reason}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
OfferIds = [erl_mesos:'OfferID'()]
Operations = [erl_mesos:'Offer.Operation'()]
Filters = undefined | erl_mesos:'Filters'()
Reason = term()
```

Accept call.

#### Decline

```erlang
erl_mesos_scheduler:decline(SchedulerInfo, OfferIds) -> 
    ok | {error, Reason}.
```

```erlang
erl_mesos_scheduler:decline(SchedulerInfo, OfferIds, Filters) -> 
    ok | {error, Reason}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
OfferIds = [erl_mesos:'OfferID'()]
Filters = undefined | erl_mesos:'Filters'()
Reason = term()
```

Decline call.

#### Revive

```erlang
erl_mesos_scheduler:revive(SchedulerInfo) -> 
    ok | {error, Reason}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
Reason = term()
```

Revive call.

#### Kill

```erlang
erl_mesos_scheduler:kill(SchedulerInfo, TaskId) -> 
    ok | {error, Reason}.
```

```erlang
erl_mesos_scheduler:kill(SchedulerInfo, TaskId, AgentId) -> 
    ok | {error, Reason}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
TaskId = erl_mesos:'TaskID'()
AgentId = undefined | erl_mesos:'AgentID()'
Reason = term()
```

Kill call.

#### Shutdown

```erlang
erl_mesos_scheduler:shutdown(SchedulerInfo, ExecutorId) -> 
    ok | {error, Reason}.
```

```erlang
erl_mesos_scheduler:shutdown(SchedulerInfo, ExecutorId, AgentId) -> 
    ok | {error, Reason}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
ExecutorId = erl_mesos:'TaskID'()
AgentId = undefined | erl_mesos:'AgentID()'
Reason = term()
```

Shutdown call.

#### Acknowledge

```erlang
erl_mesos_scheduler:acknowledge(SchedulerInfo, AgentId, TaskId, Uuid) -> 
    ok | {error, Reason}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
AgentId = erl_mesos:'AgentID()'
TaskId = erl_mesos:'TaskID'()
Uuid = binary()
Reason = term()
```

Acknowledge call.

#### Reconcile

```erlang
erl_mesos_scheduler:reconcile(SchedulerInfo, CallReconcileTasks) -> 
    ok | {error, Reason}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
CallReconcileTasks = [erl_mesos:'Call.Reconcile.Task'()]
Reason = term()
```

Reconcile call.

#### Message

```erlang
erl_mesos_scheduler:reconcile(SchedulerInfo, AgentId, ExecutorId, 
                              Data) -> 
    ok | {error, Reason}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
AgentId = erl_mesos:'AgentID'()
ExecutorId = erl_mesos:'ExecutorID'()
Data = binary()
Reason = term()
```

Reconcile call.

#### Request

```erlang
erl_mesos_scheduler:request(SchedulerInfo, Requests) -> 
    ok | {error, Reason}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
Requests = [erl_mesos:'Request'()]
Reason = term()
```

Request call.

#### Suppress

```erlang
erl_mesos_scheduler:request(SchedulerInfo) -> 
    ok | {error, Reason}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
```

Suppress call.

