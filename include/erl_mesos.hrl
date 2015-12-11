%% Scheduler info.
-record(scheduler_info, {data_format :: erl_mesos_data_format:data_format(),
                         master_host :: binary(),
                         subscribed :: boolean(),
                         framework_id :: framework_id()}).

%% Offer id.
-record(offer_id, {value :: erl_mesos_obj:data_string()}).

%% Framework id.
-record(framework_id, {value :: erl_mesos_obj:data_string()}).

%% Agent id.
-record(agent_id, {value :: erl_mesos_obj:data_string()}).

%% Label.
-record(label, {key :: erl_mesos_obj:data_string(),
                value :: undefined | erl_mesos_obj:data_string()}).

%% Labels.
-record(labels, {labels :: [label()] | erl_mesos_obj:data_obj()}).

%% Framework info.
-record(framework_info, {%% Used to determine the Unix user that an executor or
                         %% task shouldbe launched as. If the user field is set
                         %% to an empty string Mesos will automagically set it
                         %% to the current user.
                         user = <<>> :: erl_mesos_obj:data_string(),

                         %% Name of the framework that shows up in the Mesos Web
                         %% UI.
                         name = <<>> :: erl_mesos_obj:data_string(),

                         %% Note that 'id' is only available after a framework
                         %% has registered, however, it is included here in
                         %% order to facilitate scheduler failover (i.e., if it
                         %% is set then the MesosSchedulerDriver expects the
                         %% scheduler is performing failover).
                         id :: undefined | framework_id() |
                               erl_mesos_obj:data_obj(),

                         %% The amount of time that the master will wait for the
                         %% scheduler to failover before it tears down the
                         %% framework by killing all its tasks/executors. This
                         %% should be non-zero if a framework expects to
                         %% reconnect after a failover and not lose its
                         %% tasks/executors.
                         failover_timeout :: undefined | float(),

                         %% If set, framework pid, executor pids and status
                         %% updates are checkpointed to disk by the agents.
                         %% Checkpointing allows a restarted agent to reconnect
                         %% with old executors and recover status updates, at
                         %% the cost of disk I/O.
                         checkpoint :: undefined | boolean(),

                         %% Used to group frameworks for allocation decisions,
                         %% depending on the allocation policy being used.
                         role :: undefined | erl_mesos_obj:data_string(),

                         %% Used to indicate the current host from which the
                         %% scheduler is registered in the Mesos Web UI. If set
                         %% to an empty string Mesos will automagically set it
                         %% to the current hostname if one is available.
                         hostname :: undefined | erl_mesos_obj:data_string(),

                         %% This field should match the credential's principal
                         %% the framework uses for authentication. This field is
                         %% used for framework API
                         %% rate limiting and dynamic reservations. It should be
                         %% seteven if authentication is not enabled if these
                         %% features are desired.
                         principal :: undefined | erl_mesos_obj:data_string(),

                         %% This field allows a framework to advertise its web
                         %% UI, so that the Mesos web UI can link to it. It is
                         %% expected to be a full URL, for example
                         %% http://my-scheduler.example.com:8080/.
                         webui_url :: undefined | erl_mesos_obj:data_string(),

                         %% This field allows a framework to advertise its set
                         %% of capabilities (e.g., ability to receive offers for
                         %% revocable resources).
                         capabilities :: undefined |
                                         framework_info_capabilitie() |
                                         erl_mesos_obj:data_obj(),

                         %% Labels are free-form key value pairs supplied by the
                         %% framework scheduler (e.g., to describe additional
                         %% functionality offered by the framework). These
                         %% labels are not interpreted by Mesos itself.
                         labels :: undefined | labels() |
                                   erl_mesos_obj:data_obj()}).

-record(framework_info_capabilitie, {type :: erl_mesos_obj:data_string()}).

%% Value.
-record(value, {type :: value_type(),
                scalar :: undefined | value_scalar(),
                ranges :: undefined | value_ranges(),
                set :: undefined | value_set(),
                text :: value_text()}).

%% Value type.
-record(value_type, {type :: erl_mesos_obj:data_string()}).

%% Value scalar.
-record(value_scalar, {value :: float()}).

%% Value range.
-record(value_range, {'begin' :: non_neg_integer(),
                      'end' :: non_neg_integer()}).

%% Value ranges.
-record(value_ranges, {range :: [value_range() | erl_mesos_obj:data_obj()]}).

%% Value set.
-record(value_set, {item :: erl_mesos_obj:data_string()}).

%% Value text.
-record(value_text, {value :: erl_mesos_obj:data_string()}).

%% Resource.
-record(resource, {name :: erl_mesos_obj:data_string(),
                   type :: erl_mesos_obj:data_string(),
                   scalar :: undefined | value_scalar() |
                             erl_mesos_obj:data_obj(),
                   ranges :: undefined | value_ranges() |
                             erl_mesos_obj:data_obj(),
                   set :: undefined | value_set() | erl_mesos_obj:data_obj(),
                   role :: undefined | erl_mesos_obj:data_string()}).

%% Offer.
-record(offer, {id :: offer_id() | erl_mesos_obj:data_obj(),
                framework_id :: framework_id() | erl_mesos_obj:data_obj(),
                agent_id :: agent_id() | erl_mesos_obj:data_obj(),
                hostname :: erl_mesos_obj:data_string(),
                url :: undefined | erl_mesos_obj:data_obj(),
                resources :: [resource() | erl_mesos_obj:data_obj()]}).

%% Subscribed event.
-record(subscribed_event, {framework_id :: framework_id() |
                                           erl_mesos_obj:data_obj(),
                           heartbeat_interval_seconds :: undefined |
                                                         pos_integer()}).

%% Offers event.
-record(offers_event, {offers :: [offer()]}).

%% Error event.
-record(error_event, {message :: erl_mesos_obj:data_string()}).

-type offer_id() :: #offer_id{}.

-type framework_id() :: #framework_id{}.

-type agent_id() :: #agent_id{}.

-type label() :: #label{}.

-type labels() :: #labels{}.

-type scheduler_info() :: #scheduler_info{}.

-type framework_info() :: #framework_info{}.

-type framework_info_capabilitie() :: #framework_info_capabilitie{}.

-type value() :: #value{}.

-type value_type() :: #value_type{}.

-type value_scalar() :: #value_scalar{}.

-type value_range() :: #value_range{}.

-type value_ranges() :: #value_ranges{}.

-type value_set() :: #value_set{}.

-type value_text() :: #value_text{}.

-type resource() :: #resource{}.

-type offer() :: #offer{}.

-type subscribed_event() :: #subscribed_event{}.

-type offers_event() :: #offers_event{}.

-type error_event() :: #error_event{}.
