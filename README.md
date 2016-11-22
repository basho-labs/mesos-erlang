# Mesos Erlang library

## Scheduler

`erl_mesos_scheduler` process a wrapper on top of `gen_server` process.

### Starting scheduler

Scheduler process may be started by custom supervisor by
calling `erl_mesos_scheduler:start_link/4`:

```erlang
{ok, Pid} = erl_mesos_scheduler:start_link(Name, Scheduler, SchedulerOptions, Options)

Name = atom()
Scheduler = module()
SchedulerOptions = term()
Options = erl_mesos_scheduler:options()
```

* `Name` is a scheduler name. Scheduler process will be registered with this name.
* `Scheduler` is a scheduler module which must implement `erl_mesos_scheduler` behaviour.
* `SchedulerOptions` is a term which will be passed to the `Scheduler:init/1`.
* `Options` is a scheduler options.

### Scheduler options.

 Name                      | Default value          | Possible types
---------------------------|------------------------|------------------------
| master_hosts             | [<<"localhost:5000">>] | [string() \| binary()]
| heartbeat_timeout_window | 5000                   | pos_integer()
| request_options          | []                     | [{atom, term()}]
| max_num_resubscribe      | 1                      | non_neg_integer()
| resubscribe_interval     | 0                      | non_neg_integer()

`master_hosts` - list of hosts which scheduler will use during subscription
 or resubscription.

`heartbeat_timeout_window` - additional timeout value for waiting for heartbeat Mesos event.

`request_options` - HTTP request options. See https://github.com/benoitc/hackney for details.

Each time when scheduler disconnect from current mesos master it will
try to resubscribe to each host from `master_hosts` lists
`max_num_resubscribe` times with `resubscribe_interval`.

### Stopping scheduler

```erlang
ok = erl_mesos_scheduler:stop(Name)

Name = atom()
```

```erlang
ok = erl_mesos_scheduler:stop(Name, Timeout)

Timeout = timeout()
Name = atom()
```

### Scheduler callbacks

Each scheduler handler must implement `erl_mesos_scheduler` behaviour.

`erl_mesos_scheduler` callbacks:

#### Module:init/1

```erlang
Module:init(Options) ->
    {ok, FrameworkInfo, State} | {stop, Reason}.
```

Data types:

```erlang
Options = term()
FrameworkInfo = erl_mesos:'FrameworkInfo'()
State = term()
Reason = term()
```

Whenever a `erl_mesos_scheduler` process is started using
`erl_mesos_scheduler:start_link/4` this function is called by the new
process to initialize the framework.

#### Module:registered/3

```erlang
Module:registered(SchedulerInfo, EventSubscribed, State) ->
    {ok, NewState} | {stop, NewState}
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
EventSubscribed = erl_mesos_scheduler:'Event.Subscribed'()
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

#### Module:resource_offers/3

```erlang
Module:resource_offers(SchedulerInfo, EventOffers, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
EventOffers = erl_mesos_scheduler:'Event.Offers'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_scheduler` process receives offers event from
Mesos this function is called to handle this event.

#### Module:resource_resource_inverse_offers/3

```erlang
Module:resource_inverse_offers(SchedulerInfo, EventInverseOffers,
                               State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
EventInverseOffers = erl_mesos_scheduler:'Event.InverseOffers'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_scheduler` process receives inverse offers event
from Mesos this function is called to handle this event.

#### Module:offer_rescinded/3

```erlang
Module:offer_rescinded(SchedulerInfo, EventRescind, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
EventRescind = erl_mesos_scheduler:'Event.Rescind'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_scheduler` process receives rescind event from
Mesos this function is called to handle this event.

#### Module:inverse_offer_rescinded/3

```erlang
Module:inverse_offer_rescinded(SchedulerInfo, EventRescindInverseOffer,
                               State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
EventRescindInverseOffer = erl_mesos_scheduler:'Event.RescindInverseOffer'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_scheduler` process receives rescind inverse offer
event from Mesos this function is called to handle this event.

#### Module:status_update/3

```erlang
Module:status_update(SchedulerInfo, EventUpdate, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
EventUpdate = erl_mesos_scheduler:'Event.Update'()
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
EventMessage = erl_mesos_scheduler:'Event.Message'()
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
EventFailure = erl_mesos_scheduler:'Event.Failure'()
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
EventFailure = erl_mesos_scheduler:'Event.Failure'()
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
EventError = erl_mesos_scheduler:'Event.Error'()
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
Info = term()
State = term()
NewState = term()
```

This function is called by a `erl_mesos_scheduler` when it receives any
other message than Mesos event.

#### Module:terminate/2

```erlang
Module:terminate(Reason, State) ->
    Result.
```

Data types:

```erlang
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
erl_mesos_scheduler:accept(SchedulerInfo, OfferIds, Operations, Filters) ->
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

#### Accept inverse offers

```erlang
erl_mesos_scheduler:accept_inverse_offers(SchedulerInfo, OfferIds) ->
    ok | {error, Reason}.
```

```erlang
erl_mesos_scheduler:accept_inverse_offers(SchedulerInfo, OfferIds, Filters) ->
    ok | {error, Reason}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
OfferIds = [erl_mesos:'OfferID'()]
Filters = undefined | erl_mesos:'Filters'()
Reason = term()
```

Accept inverse offers call.

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

#### Decline inverse offers

```erlang
erl_mesos_scheduler:decline_inverse_offers(SchedulerInfo, OfferIds) ->
    ok | {error, Reason}.
```

```erlang
erl_mesos_scheduler:decline_inverse_offers(SchedulerInfo, OfferIds, Filters) ->
    ok | {error, Reason}.
```

Data types:

```erlang
SchedulerInfo = erl_mesos_scheduler:scheduler_info()
OfferIds = [erl_mesos:'OfferID'()]
Filters = undefined | erl_mesos:'Filters'()
Reason = term()
```

Decline inverse offers call

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
erl_mesos_scheduler:reconcile(SchedulerInfo, AgentId, ExecutorId, Data) ->
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

## Executor

`erl_mesos_executor` process a wrapper on top of `gen_server` process.

### Starting executor

Executor process may be started by custom supervisor by
calling `erl_mesos_executor:start_link/4`:

```erlang
{ok, Pid} = erl_mesos_executor:start_link(Name, Executor, ExecutorOptions, Options)

Name = term()
Executor = module()
ExecutorOptions = term()
Options = erl_mesos_executor:options()
```

* `Name` is an executor name. Executor process will be registered with this name.
* `Executor` is a executor module which must implement `erl_mesos_executor` behaviour.
* `ExecutorOptions` is a term which will be passed to the `Executor:init/1`.
* `Options` is a executor options.

### Executor options.

 Name                      | Default value          | Possible types
---------------------------|------------------------|------------------------
| request_options          | []                     | [{atom, term()}]

`request_options` - HTTP request options. See https://github.com/benoitc/hackney for details.

### Stopping executor

```erlang
ok = erl_mesos_executor:stop(Name)

Name = atom()
```

```erlang
ok = erl_mesos_executor:stop(Name, Timeout)

Timeout = timeout()
Name = atom()
```

### Executor callbacks

Each executor handler must implement `erl_mesos_executor` behaviour.

`erl_mesos_executor` callbacks:

#### Module:init/1

```erlang
Module:init(Options) ->
    {ok, CallSubscribe, State} | {stop, Reason}.
```

Data types:

```erlang
Options = term()
CallSubscribe = erl_mesos_executor:'Call.Subscribe'()
State = term()
Reason = term()
```

Whenever a `erl_mesos_executor` process is started using
`erl_mesos_executor:start_link/4` this function is called by the new
process to initialize the executor.

#### Module:registered/3

-callback registered(executor_info(), 'Event.Subscribed'(), term()) ->
    {ok, term()} | {stop, term()}.

```erlang
Module:registered(ExecutorInfo, EventSubscribed, State) ->
    {ok, NewState} | {stop, NewState}
```

Data types:

```erlang
ExecutorInfo = erl_mesos_executor:executor_info()
EventSubscribed = erl_mesos_executor:'Event.Subscribed'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_executor` process receives subscribed event from
Mesos this function is called to handle this event.

#### Module:disconnected/2

```erlang
Module:disconnected(ExecutorInfo, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
ExecutorInfo = erl_mesos_executor:executor_info()
State = term()
NewState = term()
```

Whenever a `erl_mesos_executor` process disconnected from
Mesos this function is called to handle the disconnection.
Then the `erl_mesos_executor` process will try to reconnect to the
Mesos slave if checkpoint is enabled.

#### Module:reregister/2

```erlang
Module:reregister(ExecutorInfo, State) ->
    {ok, CallSubscribe, NewState} | {stop, NewState}.
```

Data types:

```erlang
ExecutorInfo = erl_mesos_executor:executor_info()
State = term()
CallSubscribe = erl_mesos_executor:'Call.Subscribe'()
NewState = term()
```

Whenever a `erl_mesos_executor` process will try to reregistered to the
Mesos slave this function is called before registration to initialize
it.

#### Module:reregistered/2

```erlang
Module:reregistered(ExecutorInfo, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
ExecutorInfo = erl_mesos_executor:executor_info()
State = term()
NewState = term()
```

Whenever a `erl_mesos_executor` process reregistered to the Mesos slave
this function is called to handle the registration.

#### Module:launch_task/3

```erlang
Module:launch_task(ExecutorInfo, EventLaunch, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
ExecutorInfo = erl_mesos_executor:executor_info()
EventLaunch = erl_mesos_executor:'Event.Launch'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_executor` process receives launch task event from
Mesos this function is called to handle this event.

#### Module:kill_task/3

```erlang
Module:kill_task(ExecutorInfo, EventKill, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
ExecutorInfo = erl_mesos_executor:executor_info()
EventKill = erl_mesos_executor:'Event.Kill'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_executor` process receives kill task event from
Mesos this function is called to handle this event.

#### Module:acknowledged/3

```erlang
Module:acknowledged(ExecutorInfo, EventAcknowledged, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
ExecutorInfo = erl_mesos_executor:executor_info()
EventAcknowledged = erl_mesos_executor:'Event.Acknowledged'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_executor` process receives acknowledged event from
Mesos this function is called to handle this event.

#### Module:framework_message/3

```erlang
Module:framework_message(ExecutorInfo, EventMessage, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
ExecutorInfo = erl_mesos_executor:executor_info()
EventMessage = erl_mesos_executor:'Event.Message'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_executor` process receives framework message event from
Mesos this function is called to handle this event.

#### Module:error/3

```erlang
Module:error(ExecutorInfo, EventError, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
ExecutorInfo = erl_mesos_executor:executor_info()
EventError = erl_mesos_executor:'Event.Error'()
State = term()
NewState = term()
```

Whenever a `erl_mesos_executor` process receives error event from
Mesos this function is called to handle this event.

#### Module:shutdown/2

```erlang
Module:shutdown(ExecutorInfo, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
ExecutorInfo = erl_mesos_executor:executor_info()
State = term()
NewState = term()
```

Whenever a `erl_mesos_executor` process receives shutdown event from
Mesos this function is called to handle this event.

#### Module:handle_info/3

```erlang
Module:handle_info(ExecutorInfo, Info, State) ->
    {ok, NewState} | {stop, NewState}.
```

Data types:

```erlang
ExecutorInfo = erl_mesos_executor:executor_info()
Info = term()
State = term()
NewState = term()
```

This function is called by a `erl_mesos_executor` it receives any other
message than Mesos event.

#### Module:terminate/2

```erlang
Module:terminate(Reason, State) ->
    Result.
```

Data types:

```erlang
Reason = term()
State = term()
Result = term()
```

This function is called by a `erl_mesos_executor` when it is about to
terminate.

### Executor calls

#### Update

```erlang
erl_mesos_executor:update(ExecutorInfo, TaskStatus) ->
    ok | {error, Reason}.
```

Data types:

```erlang
ExecutorInfo = erl_mesos_executor:executor_info()
TaskStatus = erl_mesos:'TaskStatus'()
Reason = term()
```

Update call.

#### Message

```erlang
erl_mesos_executor:message(ExecutorInfo, Data) ->
    ok | {error, Reason}.
```

Data types:

```erlang
ExecutorInfo = erl_mesos_executor:executor_info()
Data = binary()
Reason = term()
```

Message call.
