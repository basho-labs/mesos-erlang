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
Module:disconnected(SchedulerInfo(), State) ->
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
Module:reregistered(SchedulerInfo(), State) ->
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
