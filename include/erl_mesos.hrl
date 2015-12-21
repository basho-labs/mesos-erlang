%% Address.
-record(address, {hostname :: undefined | erl_mesos_obj:data_string(),
                  ip :: undefined | erl_mesos_obj:data_string(),
                  port :: pos_integer()}).

%% Agent id.
-record(agent_id, {value :: erl_mesos_obj:data_string()}).

%% Attribute.
-record(attribute, {name :: erl_mesos_obj:data_string(),
                    type :: erl_mesos_obj:data_string(),
                    scalar :: undefined | value_scalar() |
                              erl_mesos_obj:data_obj(),
                    ranges :: undefined | value_ranges() |
                              erl_mesos_obj:data_obj(),
                    set :: undefined | value_set() | erl_mesos_obj:data_obj(),
                    text :: undefined | value_text() |
                            erl_mesos_obj:data_obj()}).

%% Call.
-record(call, {framework_id :: undefined | framework_id() |
                               erl_mesos_obj:data_obj(),
               type :: erl_mesos_obj:data_string(),
               subscribe :: undefined | call_subscribe() |
                            erl_mesos_obj:data_obj(),
               accept :: undefined | call_accept() |
                         erl_mesos_obj:data_obj()}).

%% Call subscribe.
-record(call_subscribe, {framework_info :: framework_info() |
                                           erl_mesos_obj:data_obj(),
                         force :: undefined | boolean()}).

%% Call accept.
-record(call_accept, {offer_ids :: undefined |
                                   [offer_id() | erl_mesos_obj:data_obj()],
                      operations :: undefined | [offer_operation() |
                                                 erl_mesos_obj:data_obj()],
                      filters :: undefined | erl_mesos_obj:data_obj()}).

%% Command info.
-record(command_info, {container :: undefined | command_info_container_info() |
                                    erl_mesos_obj:data_obj(),
                       uris :: undefined | [command_info_uri() |
                                            erl_mesos_obj:data_obj()],
                       environment :: undefined | environment() |
                                      erl_mesos_obj:data_obj(),
                       shell :: undefined | boolean(),
                       value :: undefined | erl_mesos_obj:data_string(),
                       arguments :: undefined | [erl_mesos_obj:data_string()],
                       user :: undefined | erl_mesos_obj:data_string()}).

%% Command info uri.
-record(command_info_uri, {value :: erl_mesos_obj:data_string(),
                           executable :: undefined | boolean(),
                           extract :: undefined | boolean(),
                           cache :: undefined | boolean()}).

%% Command info container info.
-record(command_info_container_info, {image :: erl_mesos_obj:data_string(),
                                      options :: undefined |
                                          [erl_mesos_obj:data_string()]}).

%% Container status.
-record(container_status, {network_infos :: [network_info() |
                                             erl_mesos_obj:data_obj()]}).

%% Discovery info.
-record(discovery_info, {visibility :: erl_mesos_obj:data_string(),
                         name :: undefined | erl_mesos_obj:data_string(),
                         environment :: undefined |
                                        erl_mesos_obj:data_string(),
                         location :: undefined | erl_mesos_obj:data_string(),
                         version :: undefined | erl_mesos_obj:data_string(),
                         ports :: undefined | pts() | erl_mesos_obj:data_obj(),
                         labels :: undefined | labels() |
                                   erl_mesos_obj:data_obj()}).

%% Duration info.
-record(duration_info, {nanoseconds :: non_neg_integer}).

%% Environment.
-record(environment, {variables :: [environment_variable() |
                                    erl_mesos_obj:data_obj()]}).

%% Environment variable.
-record(environment_variable, {name :: erl_mesos_obj:data_string(),
                               value :: erl_mesos_obj:data_string()}).

%% Event.
-record(event, {type :: subscribed | offers | rescind | update | message |
                        failure | error | heartbeat |
                        erl_mesos_obj:data_string(),
                subscribed :: undefined | event_subscribed() |
                              erl_mesos_obj:data_obj(),
                offers :: undefined | event_offers() | erl_mesos_obj:data_obj(),
                rescind :: undefined | event_rescind() |
                           erl_mesos_obj:data_obj(),
                update :: undefined | event_update() | erl_mesos_obj:data_obj(),
                error :: undefined | event_error() | erl_mesos_obj:data_obj()}).

%% Event subscribed.
-record(event_subscribed, {framework_id :: framework_id() |
                                           erl_mesos_obj:data_obj(),
                           heartbeat_interval_seconds :: undefined | float()}).

%% Event offers.
-record(event_offers, {offers :: undefined | [offer() |
                                              erl_mesos_obj:data_obj()],
                       inverse_offers :: undefined |
                                         [inverse_offer() |
                                          erl_mesos_obj:data_obj()]}).

%% Event rescind.
-record(event_rescind, {offer_id :: offer_id() | erl_mesos_obj:data_obj()}).

%% Error update.
-record(event_update, {status :: task_status() | erl_mesos_obj:data_obj()}).

%% Error event.
-record(event_error, {message :: erl_mesos_obj:data_string()}).

%% Executor id.
-record(executor_id, {value :: erl_mesos_obj:data_string()}).

%% Framework id.
-record(framework_id, {value :: erl_mesos_obj:data_string()}).

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

%% Image.
-record(image, {type :: erl_mesos_obj:data_string(),
                appc :: undefined | image_appc() | erl_mesos_obj:data_obj(),
                %% since 0.26.0
                docker :: undefined | image_docker() |
                          erl_mesos_obj:data_obj()}).

%% Image appc.
-record(image_appc, {name :: erl_mesos_obj:data_string(),
                     id :: undefined | erl_mesos_obj:data_string(),
                     labels :: undefined | labels() |
                               erl_mesos_obj:data_obj()}).

%% Image docker.
-record(image_docker, {name :: erl_mesos_obj:data_string()}).

%% Inverse offer.
-record(inverse_offer, {id :: offer_id() | erl_mesos_obj:data_obj(),
                        url :: undefined | url() | erl_mesos_obj:data_obj(),
                        framework_id :: framework_id() |
                                        erl_mesos_obj:data_obj(),
                        agent_id :: undefined | agent_id() |
                                    erl_mesos_obj:data_obj(),
                        unavailability :: unavailability() |
                                          erl_mesos_obj:data_obj(),
                        resources :: undefined | [resource() |
                                                  erl_mesos_obj:data_obj()]}).

%% Label.
-record(label, {key :: erl_mesos_obj:data_string(),
                value :: undefined | erl_mesos_obj:data_string()}).

%% Labels.
-record(labels, {labels :: [label() | erl_mesos_obj:data_obj()]}).

%% Network info.
-record(network_info, {ip_addresses :: undefined | [network_info_ip_address() |
                                                    erl_mesos_obj:data_obj()],
                       %% deprecated since 0.26.0
                       protocol :: undefined | erl_mesos_obj:data_string(),
                       %% deprecated since 0.26.0
                       ip_address :: undefined | erl_mesos_obj:data_string(),
                       groups :: undefined | [erl_mesos_obj:data_string()],
                       labels :: undefined | labels() |
                                 erl_mesos_obj:data_obj()}).

%% Network info ip address.
-record(network_info_ip_address, {protocol :: undefined |
                                              erl_mesos_obj:data_string(),
                                  ip_address :: undefined |
                                                erl_mesos_obj:data_string()}).

%% Offer.
-record(offer, {id :: offer_id() | erl_mesos_obj:data_obj(),
                framework_id :: framework_id() | erl_mesos_obj:data_obj(),
                agent_id :: agent_id() | erl_mesos_obj:data_obj(),
                hostname :: erl_mesos_obj:data_string(),
                url :: undefined | url() | erl_mesos_obj:data_obj(),
                resources :: undefined | [resource() |
                                          erl_mesos_obj:data_obj()],
                attributes :: undefined | [attribute() |
                                           erl_mesos_obj:data_obj()],
                executor_ids :: undefined | [executor_id() |
                                             erl_mesos_obj:data_obj()],
                unavailability :: undefined | unavailability() |
                                  erl_mesos_obj:data_obj()}).

%% Offer operation.
-record(offer_operation, {type :: erl_mesos_obj:data_string(),
                          launch :: undefined | offer_operation_launch() |
                                    erl_mesos_obj:data_obj(),
                          reserve :: undefined | offer_operation_reserve() |
                                     erl_mesos_obj:data_obj(),
                          unreserve :: undefined | offer_operation_unreserve() |
                                       erl_mesos_obj:data_obj(),
                          create :: undefined | offer_operation_create() |
                                    erl_mesos_obj:data_obj(),
                          destroy :: undefined | offer_operation_destroy() |
                                     erl_mesos_obj:data_obj()}).

%% Offer operation launch.
-record(offer_operation_launch, {task_infos :: [task_info() |
                                                erl_mesos_obj:data_obj()]}).

%% Offer operation reserve.
-record(offer_operation_reserve, {resources :: [resource() |
                                                erl_mesos_obj:data_obj()]}).

%% Offer operation unreserve.
-record(offer_operation_unreserve, {resources :: [resource() |
                                                  erl_mesos_obj:data_obj()]}).

%% Offer operation create.
-record(offer_operation_create, {volumes :: [resource() |
                                             erl_mesos_obj:data_obj()]}).

%% Offer operation destroy.
-record(offer_operation_destroy, {volumes :: [resource() |
                                              erl_mesos_obj:data_obj()]}).

%% Offer id.
-record(offer_id, {value :: erl_mesos_obj:data_string()}).

%% Parameter.
-record(parameter, {key :: erl_mesos_obj:data_string(),
                    value :: erl_mesos_obj:data_string()}).

%% Port.
-record(port, {number :: integer(),
               name :: undefined | erl_mesos_obj:data_string(),
               protocol :: undefined | erl_mesos_obj:data_string()}).

%% Ports.
-record(ports, {ports :: [pt() | erl_mesos_obj:data_obj()]}).

%% Resource.
-record(resource, {name :: erl_mesos_obj:data_string(),
                   type :: erl_mesos_obj:data_string(),
                   scalar :: undefined | value_scalar() |
                             erl_mesos_obj:data_obj(),
                   ranges :: undefined | value_ranges() |
                             erl_mesos_obj:data_obj(),
                   set :: undefined | value_set() | erl_mesos_obj:data_obj(),
                   role :: undefined | erl_mesos_obj:data_string(),
                   reservation :: undefined | resource_reservation_info() |
                                  erl_mesos_obj:data_obj(),
                   disk :: undefined | resource_disk_info() |
                           erl_mesos_obj:data_obj(),
                   revocable :: undefined | resource_revocable_info() |
                                erl_mesos_obj:data_obj()}).

%% Resource reservation info.
-record(resource_reservation_info, {principal :: erl_mesos_obj:data_string()}).

%% Resource disk info.
-record(resource_disk_info, {persistence :: undefined |
                                            resource_disk_info_persistence() |
                                            erl_mesos_obj:data_obj(),
                             volume :: undefined | volume() |
                                       erl_mesos_obj:data_obj()}).

%% Resource disk info persistence.
-record(resource_disk_info_persistence, {id :: erl_mesos_obj:data_string()}).

%% Resource revocable info.
-record(resource_revocable_info, {}).

%% Scheduler info.
-record(scheduler_info, {data_format :: erl_mesos_data_format:data_format(),
                         api_version :: erl_mesos_scheduler_call:version(),
                         master_host :: binary(),
                         request_options :: erl_mesos_http:options(),
                         subscribed :: boolean(),
                         framework_id :: framework_id()}).

%% Task id.
-record(task_id, {value :: erl_mesos_obj:data_string()}).

%% Task info.
-record(task_info, {name :: erl_mesos_obj:data_string(),
                    task_id :: task_id() | erl_mesos_obj:data_obj(),
                    agent_id :: agent_id() | erl_mesos_obj:data_obj(),
                    resources :: undefined | [resource() |
                                              erl_mesos_obj:data_obj()]}).

%% Task status.
-record(task_status, {task_id :: task_id() | erl_mesos_obj:data_obj(),
                      state :: erl_mesos_obj:data_string(),
                      message :: undefined | erl_mesos_obj:data_string(),
                      source :: undefined | erl_mesos_obj:data_string(),
                      reason :: undefined | erl_mesos_obj:data_string(),
                      data :: undefined | erl_mesos_obj:data_string(),
                      agent_id :: undefined | agent_id() |
                                  erl_mesos_obj:data_obj(),
                      executor_id :: undefined | executor_id() |
                                     erl_mesos_obj:data_obj(),
                      timestamp :: undefined | float(),
                      uuid :: undefined | erl_mesos_obj:data_string(),
                      healthy :: undefined | boolean(),
                      labels :: undefined | labels() | erl_mesos_obj:data_obj(),
                      %% since 0.26.0
                      container_status :: undefined | container_status() |
                                          erl_mesos_obj:data_obj()}).

%% Time info.
-record(time_info, {nanoseconds :: non_neg_integer()}).

%% Unavailability.
-record(unavailability, {start :: time_info() | erl_mesos_obj:data_obj(),
                         duration :: undefined | duration_info() |
                                     erl_mesos_obj:data_obj()}).

%% Url.
-record(url, {scheme :: erl_mesos_obj:data_string(),
              address :: address() | erl_mesos_obj:data_obj(),
              path :: undefined | erl_mesos_obj:data_string(),
              query :: undefined | [parameter() | erl_mesos_obj:data_obj()],
              fragment :: undefined | erl_mesos_obj:data_string()}).

%% Value.
-record(value, {type :: value_type(),
                scalar :: undefined | value_scalar() | erl_mesos_obj:data_obj(),
                ranges :: undefined | value_ranges() | erl_mesos_obj:data_obj(),
                set :: undefined | value_set() | erl_mesos_obj:data_obj(),
                text :: undefined | value_text() | erl_mesos_obj:data_obj()}).

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

%% Volume.
-record(volume, {mode :: erl_mesos_obj:data_string(),
                 container_path :: erl_mesos_obj:data_string(),
                 host_path :: undefined | erl_mesos_obj:data_string(),
                 image :: undefined | image() | erl_mesos_obj:data_obj()}).

-type address() :: #address{}.
-export_type([address/0]).

-type agent_id() :: #agent_id{}.
-export_type([agent_id/0]).

-type attribute() :: #attribute{}.
-export_type([attribute/0]).

-type call() :: #call{}.
-export_type([call/0]).

-type call_subscribe() :: #call_subscribe{}.
-export_type([call_subscribe/0]).

-type call_accept() :: #call_accept{}.
-export_type([call_accept/0]).

-type command_info() :: #command_info{}.
-export_type([command_info/0]).

-type command_info_uri() :: #command_info_uri{}.
-export_type([command_info_uri/0]).

-type command_info_container_info() :: #command_info_container_info{}.
-export_type([command_info_container_info/0]).

-type container_status() :: #container_status{}.
-export_type([container_status/0]).

-type discovery_info() :: #discovery_info{}.
-export_type([discovery_info/0]).

-type duration_info() :: #duration_info{}.
-export_type([duration_info/0]).

-type environment() :: #environment{}.
-export_type([environment/0]).

-type environment_variable() :: #environment_variable{}.
-export_type([environment_variable/0]).

-type event() :: #event{}.
-export_type([event/0]).

-type event_subscribed() :: #event_subscribed{}.
-export_type([event_subscribed/0]).

-type event_offers() :: #event_offers{}.
-export_type([event_offers/0]).

-type event_rescind() :: #event_rescind{}.
-export_type([event_rescind/0]).

-type event_update() :: #event_update{}.
-export_type([event_update/0]).

-type event_error() :: #event_error{}.
-export_type([event_error/0]).

-type executor_id() :: #executor_id{}.
-export_type([executor_id/0]).

-type framework_id() :: #framework_id{}.
-export_type([framework_id/0]).

-type framework_info() :: #framework_info{}.
-export_type([framework_info/0]).

-type framework_info_capabilitie() :: #framework_info_capabilitie{}.
-export_type([framework_info_capabilitie/0]).

-type image() :: #image{}.
-export_type([image/0]).

-type image_appc() :: #image_appc{}.
-export_type([image_appc/0]).

-type image_docker() :: #image_docker{}.
-export_type([image_docker/0]).

-type inverse_offer() :: #inverse_offer{}.
-export_type([inverse_offer/0]).

-type label() :: #label{}.
-export_type([label/0]).

-type labels() :: #labels{}.
-export_type([labels/0]).

-type network_info() :: #network_info{}.
-export_type([network_info/0]).

-type network_info_ip_address() :: #network_info_ip_address{}.
-export_type([network_info_ip_address/0]).

-type offer() :: #offer{}.
-export_type([offer/0]).

-type offer_operation() :: #offer_operation{}.
-export_type([offer_operation/0]).

-type offer_operation_launch() :: #offer_operation_launch{}.
-export_type([offer_operation_launch/0]).

-type offer_operation_reserve() :: #offer_operation_reserve{}.
-export_type([offer_operation_reserve/0]).

-type offer_operation_unreserve() :: #offer_operation_unreserve{}.
-export_type([offer_operation_unreserve/0]).

-type offer_operation_create() :: #offer_operation_create{}.
-export_type([offer_operation_create/0]).

-type offer_operation_destroy() :: #offer_operation_destroy{}.
-export_type([offer_operation_destroy/0]).

-type offer_id() :: #offer_id{}.
-export_type([offer_id/0]).

-type pt() :: #port{}.
-export_type([pt/0]).

-type pts() :: #ports{}.
-export_type([pts/0]).

-type parameter() :: #parameter{}.
-export_type([parameter/0]).

-type resource() :: #resource{}.
-export_type([resource/0]).

-type resource_reservation_info() :: #resource_reservation_info{}.
-export_type([resource_reservation_info/0]).

-type resource_disk_info() :: #resource_disk_info{}.
-export_type([resource_disk_info/0]).

-type resource_disk_info_persistence() :: #resource_disk_info_persistence{}.
-export_type([resource_disk_info_persistence/0]).

-type resource_revocable_info() :: #resource_revocable_info{}.
-export_type([resource_revocable_info/0]).

-type scheduler_info() :: #scheduler_info{}.
-export_type([scheduler_info/0]).

-type task_id() :: #task_id{}.
-export_type([task_id/0]).

-type task_info() :: #task_info{}.
-export_type([task_info/0]).

-type task_status() :: #task_status{}.
-export_type([task_status/0]).

-type time_info() :: #time_info{}.
-export_type([time_info/0]).

-type unavailability() :: #unavailability{}.
-export_type([unavailability/0]).

-type url() :: #url{}.
-export_type([url/0]).

-type value() :: #value{}.
-export_type([value/0]).

-type value_type() :: #value_type{}.
-export_type([value_type/0]).

-type value_scalar() :: #value_scalar{}.
-export_type([value_scalar/0]).

-type value_range() :: #value_range{}.
-export_type([value_range/0]).

-type value_ranges() :: #value_ranges{}.
-export_type([value_ranges/0]).

-type value_set() :: #value_set{}.
-export_type([value_set/0]).

-type value_text() :: #value_text{}.
-export_type([value_text/0]).

-type volume() :: #volume{}.
-export_type([volume/0]).
