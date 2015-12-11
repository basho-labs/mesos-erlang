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
-record(framework_info, {user = <<>> :: erl_mesos_obj:data_string(),
                         name = <<>> :: erl_mesos_obj:data_string(),
                         id :: undefined | framework_id() |
                               erl_mesos_obj:data_obj(),
                         failover_timeout :: undefined | float(),
                         checkpoint :: undefined | boolean(),
                         role :: undefined | erl_mesos_obj:data_string(),
                         hostname :: undefined | erl_mesos_obj:data_string(),
                         principal :: undefined | erl_mesos_obj:data_string(),
                         webui_url :: undefined | erl_mesos_obj:data_string(),
                         capabilities :: undefined |
                                         framework_info_capabilitie() |
                                         erl_mesos_obj:data_obj(),
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

%% Address.
-record(address, {hostname :: undefined | erl_mesos_obj:data_string(),
                  ip :: undefined | erl_mesos_obj:data_string(),
                  port :: pos_integer()}).

%% Parameter.
-record(parameter, {key :: erl_mesos_obj:data_string(),
                    value :: erl_mesos_obj:data_string()}).

%% Url.
-record(url, {scheme :: erl_mesos_obj:data_string(),
              address :: address() | erl_mesos_obj:data_obj(),
              path :: undefined | erl_mesos_obj:data_string(),
              query :: undefined | [parameter() | erl_mesos_obj:data_obj()],
              fragment :: undefined | erl_mesos_obj:data_string()}).

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
                url :: undefined | url() | erl_mesos_obj:data_obj(),
                resources :: undefined | [resource() |
                             erl_mesos_obj:data_obj()]}).

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

-type address() :: #address{}.

-type parameter() :: #parameter{}.

-type url() :: #url{}.

-type resource() :: #resource{}.

-type offer() :: #offer{}.

-type subscribed_event() :: #subscribed_event{}.

-type offers_event() :: #offers_event{}.

-type error_event() :: #error_event{}.
