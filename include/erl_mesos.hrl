%% Address.
-record(address, {hostname :: undefined | erl_mesos_obj:data_string(),
                  ip :: undefined | erl_mesos_obj:data_string(),
                  port :: pos_integer()}).

%% Agent id.
-record(agent_id, {value :: erl_mesos_obj:data_string()}).

%% Attribute.
-record(attribute, {name :: erl_mesos_obj:data_string(),
                    type :: erl_mesos_obj:data_string(),
                    scalar :: undefined | erl_mesos:value_scalar() |
                              erl_mesos_obj:data_obj(),
                    ranges :: undefined | erl_mesos:value_ranges() |
                              erl_mesos_obj:data_obj(),
                    set :: undefined | erl_mesos:value_set() |
                           erl_mesos_obj:data_obj(),
                    text :: undefined | erl_mesos:value_text() |
                            erl_mesos_obj:data_obj()}).

%% Call.
-record(call, {framework_id :: undefined | erl_mesos:framework_id() |
                               erl_mesos_obj:data_obj(),
               type :: erl_mesos_obj:data_string(),
               subscribe :: undefined | erl_mesos:call_subscribe() |
                            erl_mesos_obj:data_obj(),
               accept :: undefined | erl_mesos:call_accept() |
                         erl_mesos_obj:data_obj(),
               decline :: undefined | erl_mesos:call_decline() |
                          erl_mesos_obj:data_obj(),
               kill :: undefined | erl_mesos:call_kill() |
                       erl_mesos_obj:data_obj(),
               shutdown :: undefined | erl_mesos:call_shutdown() |
                           erl_mesos_obj:data_obj(),
               acknowledge :: erl_mesos:call_acknowledge() |
                              erl_mesos_obj:data_obj(),
               reconcile :: undefined | erl_mesos:call_reconcile() |
                            erl_mesos_obj:data_obj(),
               message :: undefined | erl_mesos:call_message() |
                          erl_mesos_obj:data_obj(),
               request :: undefined | erl_mesos:call_request() |
                          erl_mesos_obj:data_obj()}).

%% Call subscribe.
-record(call_subscribe, {framework_info :: erl_mesos:framework_info() |
                                           erl_mesos_obj:data_obj(),
                         force :: undefined | boolean()}).

%% Call accept.
-record(call_accept, {offer_ids :: [erl_mesos:offer_id() |
                                    erl_mesos_obj:data_obj()],
                      operations :: [erl_mesos:offer_operation() |
                                     erl_mesos_obj:data_obj()],
                      filters :: undefined | erl_mesos:filters() |
                                 erl_mesos_obj:data_obj()}).

%% Call decline.
-record(call_decline, {offer_ids :: [erl_mesos:offer_id() |
                                     erl_mesos_obj:data_obj()],
                       filters :: undefined | erl_mesos:filters() |
                                  erl_mesos_obj:data_obj()}).

%% Call kill.
-record(call_kill, {task_id :: erl_mesos:task_id() |
                               erl_mesos_obj:data_obj(),
                    agent_id :: undefined | erl_mesos:agent_id() |
                                erl_mesos_obj:data_obj()}).

%% Call shutdown.
-record(call_shutdown, {executor_id :: erl_mesos:executor_id() |
                                       erl_mesos_obj:data_obj(),
                        agent_id :: erl_mesos:agent_id() |
                                    erl_mesos_obj:data_obj()}).

%% Call reconcile.
-record(call_reconcile, {tasks :: [erl_mesos:call_reconcile_task() |
                                   erl_mesos_obj:data_obj()]}).

%% Call acknowledge.
-record(call_acknowledge, {agent_id :: erl_mesos:agent_id() |
                                       erl_mesos_obj:data_obj(),
                           task_id :: erl_mesos:task_id() |
                                      erl_mesos_obj:data_obj(),
                           uuid :: erl_mesos_obj:data_string()}).

%% Call reconcile task.
-record(call_reconcile_task, {task_id :: erl_mesos:task_id() |
                                         erl_mesos_obj:data_obj(),
                              agent_id :: undefined | erl_mesos:agent_id() |
                                          erl_mesos_obj:data_obj()}).

%% Call message.
-record(call_message, {agent_id :: erl_mesos:agent_id() |
                                   erl_mesos_obj:data_obj(),
                       executor_id :: erl_mesos:executor_id() |
                                      erl_mesos_obj:data_obj(),
                       data :: erl_mesos_obj:data_string()}).

%% Call request.
-record(call_request, {requests :: [erl_mesos:request() |
                                    erl_mesos_obj:data_obj()]}).

%% Command info.
-record(command_info, {container :: undefined |
                                    erl_mesos:command_info_container_info() |
                                    erl_mesos_obj:data_obj(),
                       uris :: undefined | [erl_mesos:command_info_uri() |
                                            erl_mesos_obj:data_obj()],
                       environment :: undefined | erl_mesos:environment() |
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

%% Container info.
-record(container_info, {type :: erl_mesos_obj:data_string(),
                         volumes :: undefined | [erl_mesos:volume() |
                                                 erl_mesos_obj:data_obj()],
                         hostname :: undefined | erl_mesos_obj:data_string(),
                         docker :: undefined |
                                   erl_mesos:container_info_docker_info() |
                                   erl_mesos_obj:data_obj(),
                         mesos :: undefined |
                                  erl_mesos:container_info_mesos_info() |
                                  erl_mesos_obj:data_obj(),
                         network_infos :: undefined |
                                          [erl_mesos:network_info() |
                                           erl_mesos_obj:data_obj()]}).

%% Container info docker info.
-record(container_info_docker_info,
        {image :: erl_mesos_obj:data_string(),
         network :: undefined | erl_mesos_obj:data_string(),
         port_mappings :: undefined |
                          [erl_mesos:container_info_docker_info_port_mapping() |
                           erl_mesos_obj:data_obj()],
         privileged :: undefined | boolean(),
         parameters :: undefined | [erl_mesos:parameter() |
                                    erl_mesos_obj:data_obj()],
         force_pull_image :: undefined | boolean()}).

%% Container info docker info port mapping.
-record(container_info_docker_info_port_mapping,
        {host_port :: pos_integer(),
         container_port :: pos_integer(),
         protocol :: undefined | erl_mesos_obj:data_string()}).

%% Container info mesos info.
-record(container_info_mesos_info, {image :: undefined | erl_mesos:image() |
                                             erl_mesos_obj:data_obj()}).

%% Container status.
-record(container_status, {network_infos :: [erl_mesos:network_info() |
                                             erl_mesos_obj:data_obj()]}).

%% Discovery info.
-record(discovery_info, {visibility :: erl_mesos_obj:data_string(),
                         name :: undefined | erl_mesos_obj:data_string(),
                         environment :: undefined |
                                        erl_mesos_obj:data_string(),
                         location :: undefined | erl_mesos_obj:data_string(),
                         version :: undefined | erl_mesos_obj:data_string(),
                         ports :: undefined | erl_mesos:pts() |
                                  erl_mesos_obj:data_obj(),
                         labels :: undefined | erl_mesos:labels() |
                                   erl_mesos_obj:data_obj()}).

%% Duration info.
-record(duration_info, {nanoseconds :: non_neg_integer}).

%% Environment.
-record(environment, {variables :: [erl_mesos:environment_variable() |
                                    erl_mesos_obj:data_obj()]}).

%% Environment variable.
-record(environment_variable, {name :: erl_mesos_obj:data_string(),
                               value :: erl_mesos_obj:data_string()}).

%% Event.
-record(event, {type :: subscribed | offers | rescind | update | message |
                        failure | error | heartbeat |
                        erl_mesos_obj:data_string(),
                subscribed :: undefined | erl_mesos:event_subscribed() |
                              erl_mesos_obj:data_obj(),
                offers :: undefined | erl_mesos:event_offers() |
                          erl_mesos_obj:data_obj(),
                rescind :: undefined | erl_mesos:event_rescind() |
                           erl_mesos_obj:data_obj(),
                update :: undefined | erl_mesos:event_update() |
                          erl_mesos_obj:data_obj(),
                message :: undefined | erl_mesos:event_message() |
                           erl_mesos_obj:data_obj(),
                failure :: undefined | erl_mesos:event_failure() |
                           erl_mesos_obj:data_obj(),
                error :: undefined | erl_mesos:event_error() |
                         erl_mesos_obj:data_obj()}).

%% Event subscribed.
-record(event_subscribed, {framework_id :: erl_mesos:framework_id() |
                                           erl_mesos_obj:data_obj(),
                           heartbeat_interval_seconds :: undefined | float()}).

%% Event offers.
-record(event_offers, {offers :: undefined | [erl_mesos:offer() |
                                              erl_mesos_obj:data_obj()],
                       inverse_offers :: undefined |
                                         [erl_mesos:inverse_offer() |
                                          erl_mesos_obj:data_obj()]}).

%% Event rescind.
-record(event_rescind, {offer_id :: erl_mesos:offer_id() |
                                    erl_mesos_obj:data_obj()}).

%% Event update.
-record(event_update, {status :: erl_mesos:task_status() |
                                 erl_mesos_obj:data_obj()}).

%% Event message.
-record(event_message, {agent_id :: erl_mesos:agent_id() |
                                    erl_mesos_obj:data_obj(),
                        executor_id :: erl_mesos:executor_id() |
                                       erl_mesos_obj:data_obj(),
                        data :: erl_mesos_obj:data_string()}).

%% Event failure.
-record(event_failure, {agent_id :: undefined | erl_mesos:agent_id() |
                                    erl_mesos_obj:data_obj(),
                        executor_id :: undefined | erl_mesos:executor_id() |
                                       erl_mesos_obj:data_obj(),
                        status :: undefined | integer()}).

%% Event error.
-record(event_error, {message :: erl_mesos_obj:data_string()}).

%% Executor id.
-record(executor_id, {value :: erl_mesos_obj:data_string()}).

%% Executor info.
-record(executor_info, {executor_id :: erl_mesos:executor_id() |
                                       erl_mesos_obj:data_obj(),
                        framework_id :: undefined |
                                        erl_mesos:framework_id() |
                                        erl_mesos_obj:data_obj(),
                        command :: erl_mesos:command_info() |
                                   erl_mesos_obj:data_obj(),
                        container :: undefined |
                                     erl_mesos:container_info() |
                                     erl_mesos_obj:data_obj(),
                        resources :: undefined | [erl_mesos:resource() |
                                                  erl_mesos_obj:data_obj()],
                        name :: undefined | erl_mesos_obj:data_string(),
                        source :: undefined | erl_mesos_obj:data_string(),
                        data :: undefined | erl_mesos_obj:data_string(),
                        discovery :: undefined | erl_mesos:discovery_info() |
                                     erl_mesos_obj:data_obj()}).

%% Filters.
-record(filters, {refuse_seconds :: undefined | float()}).

%% Framework id.
-record(framework_id, {value :: erl_mesos_obj:data_string()}).

%% Framework info.
-record(framework_info, {user = <<>> :: erl_mesos_obj:data_string(),
                         name = <<>> :: erl_mesos_obj:data_string(),
                         id :: undefined | erl_mesos:framework_id() |
                               erl_mesos_obj:data_obj(),
                         failover_timeout :: undefined | float(),
                         checkpoint :: undefined | boolean(),
                         role :: undefined | erl_mesos_obj:data_string(),
                         hostname :: undefined | erl_mesos_obj:data_string(),
                         principal :: undefined | erl_mesos_obj:data_string(),
                         webui_url :: undefined | erl_mesos_obj:data_string(),
                         capabilities :: undefined |
                             erl_mesos:framework_info_capabilitie() |
                             erl_mesos_obj:data_obj(),
                         labels :: undefined | erl_mesos:labels() |
                                   erl_mesos_obj:data_obj()}).

%% Framework info capabilitie.
-record(framework_info_capabilitie, {type :: erl_mesos_obj:data_string()}).

%% Health check.
-record(health_check, {http :: undefined | erl_mesos:health_check_http() |
                               erl_mesos_obj:data_obj(),
                       delay_seconds :: undefined | float(),
                       interval_seconds :: undefined | float(),
                       timeout_seconds :: undefined | float(),
                       consecutive_failures :: undefined | pos_integer(),
                       grace_period_seconds :: undefined | float(),
                       command :: undefined | erl_mesos:command_info() |
                                  erl_mesos_obj:data_obj()}).

%% Health check http.
-record(health_check_http, {port :: pos_integer(),
                            path :: undefined | erl_mesos_obj:data_string(),
                            statuses :: undefined | [pos_integer()]}).

%% Image.
-record(image, {type :: erl_mesos_obj:data_string(),
                appc :: undefined | erl_mesos:image_appc() |
                        erl_mesos_obj:data_obj(),
                %% since 0.26.0
                docker :: undefined | erl_mesos:image_docker() |
                          erl_mesos_obj:data_obj()}).

%% Image appc.
-record(image_appc, {name :: erl_mesos_obj:data_string(),
                     id :: undefined | erl_mesos_obj:data_string(),
                     labels :: undefined | erl_mesos:labels() |
                               erl_mesos_obj:data_obj()}).

%% Image docker.
-record(image_docker, {name :: erl_mesos_obj:data_string()}).

%% Inverse offer.
-record(inverse_offer, {id :: erl_mesos:offer_id() | erl_mesos_obj:data_obj(),
                        url :: undefined | erl_mesos:url() |
                               erl_mesos_obj:data_obj(),
                        framework_id :: erl_mesos:framework_id() |
                                        erl_mesos_obj:data_obj(),
                        agent_id :: undefined | erl_mesos:agent_id() |
                                    erl_mesos_obj:data_obj(),
                        unavailability :: erl_mesos:unavailability() |
                                          erl_mesos_obj:data_obj(),
                        resources :: undefined | [erl_mesos:resource() |
                                                  erl_mesos_obj:data_obj()]}).

%% Label.
-record(label, {key :: erl_mesos_obj:data_string(),
                value :: undefined | erl_mesos_obj:data_string()}).

%% Labels.
-record(labels, {labels :: [erl_mesos:label() | erl_mesos_obj:data_obj()]}).

%% Network info.
-record(network_info, {ip_addresses :: undefined |
                                       [erl_mesos:network_info_ip_address() |
                                        erl_mesos_obj:data_obj()],
                       %% deprecated since 0.26.0
                       protocol :: undefined | erl_mesos_obj:data_string(),
                       %% deprecated since 0.26.0
                       ip_address :: undefined | erl_mesos_obj:data_string(),
                       groups :: undefined | [erl_mesos_obj:data_string()],
                       labels :: undefined | erl_mesos:labels() |
                                 erl_mesos_obj:data_obj()}).

%% Network info ip address.
-record(network_info_ip_address, {protocol :: undefined |
                                              erl_mesos_obj:data_string(),
                                  ip_address :: undefined |
                                                erl_mesos_obj:data_string()}).

%% Offer.
-record(offer, {id :: erl_mesos:offer_id() | erl_mesos_obj:data_obj(),
                framework_id :: erl_mesos:framework_id() |
                                erl_mesos_obj:data_obj(),
                agent_id :: erl_mesos:agent_id() | erl_mesos_obj:data_obj(),
                hostname :: erl_mesos_obj:data_string(),
                url :: undefined | erl_mesos:url() | erl_mesos_obj:data_obj(),
                resources :: undefined | [erl_mesos:resource() |
                                          erl_mesos_obj:data_obj()],
                attributes :: undefined | [erl_mesos:attribute() |
                                           erl_mesos_obj:data_obj()],
                executor_ids :: undefined | [erl_mesos:executor_id() |
                                             erl_mesos_obj:data_obj()],
                unavailability :: undefined | erl_mesos:unavailability() |
                                  erl_mesos_obj:data_obj()}).

%% Offer operation.
-record(offer_operation, {type :: erl_mesos_obj:data_string(),
                          launch :: undefined |
                                    erl_mesos:offer_operation_launch() |
                                    erl_mesos_obj:data_obj(),
                          reserve :: undefined |
                                     erl_mesos:offer_operation_reserve() |
                                     erl_mesos_obj:data_obj(),
                          unreserve :: undefined |
                                       erl_mesos:offer_operation_unreserve() |
                                       erl_mesos_obj:data_obj(),
                          create :: undefined |
                                    erl_mesos:offer_operation_create() |
                                    erl_mesos_obj:data_obj(),
                          destroy :: undefined |
                                     erl_mesos:offer_operation_destroy() |
                                     erl_mesos_obj:data_obj()}).

%% Offer operation launch.
-record(offer_operation_launch, {task_infos :: [erl_mesos:task_info() |
                                                erl_mesos_obj:data_obj()]}).

%% Offer operation reserve.
-record(offer_operation_reserve, {resources :: [erl_mesos:resource() |
                                                erl_mesos_obj:data_obj()]}).

%% Offer operation unreserve.
-record(offer_operation_unreserve, {resources :: [erl_mesos:resource() |
                                                  erl_mesos_obj:data_obj()]}).

%% Offer operation create.
-record(offer_operation_create, {volumes :: [erl_mesos:resource() |
                                             erl_mesos_obj:data_obj()]}).

%% Offer operation destroy.
-record(offer_operation_destroy, {volumes :: [erl_mesos:resource() |
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
-record(ports, {ports :: [erl_mesos:pt() | erl_mesos_obj:data_obj()]}).

%% Request.
-record(request, {agent_id :: undefined | erl_mesos:agent_id() |
                              erl_mesos_obj:data_obj(),
                  resources :: undefined | [erl_mesos:resource() |
                                            erl_mesos_obj:data_obj()]}).

%% Resource.
-record(resource, {name :: erl_mesos_obj:data_string(),
                   type :: erl_mesos_obj:data_string(),
                   scalar :: undefined | erl_mesos:value_scalar() |
                             erl_mesos_obj:data_obj(),
                   ranges :: undefined | erl_mesos:value_ranges() |
                             erl_mesos_obj:data_obj(),
                   set :: undefined | erl_mesos:value_set() |
                          erl_mesos_obj:data_obj(),
                   role :: undefined | erl_mesos_obj:data_string(),
                   reservation :: undefined |
                                  erl_mesos:resource_reservation_info() |
                                  erl_mesos_obj:data_obj(),
                   disk :: undefined |
                           erl_mesos:resource_disk_info() |
                           erl_mesos_obj:data_obj(),
                   revocable :: undefined |
                                erl_mesos:resource_revocable_info() |
                                erl_mesos_obj:data_obj()}).

%% Resource reservation info.
-record(resource_reservation_info, {principal :: erl_mesos_obj:data_string()}).

%% Resource disk info.
-record(resource_disk_info,
        {persistence :: undefined |
                        erl_mesos:resource_disk_info_persistence() |
                        erl_mesos_obj:data_obj(),
         volume :: undefined | erl_mesos:volume() | erl_mesos_obj:data_obj()}).

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
                         framework_id :: erl_mesos:framework_id()}).

%% Task id.
-record(task_id, {value :: erl_mesos_obj:data_string()}).

%% Task info.
-record(task_info, {name :: erl_mesos_obj:data_string(),
                    task_id :: erl_mesos:task_id() | erl_mesos_obj:data_obj(),
                    agent_id :: erl_mesos:agent_id() | erl_mesos_obj:data_obj(),
                    resources :: undefined | [erl_mesos:resource() |
                                              erl_mesos_obj:data_obj()],
                    executor :: undefined | erl_mesos:executor_info() |
                                erl_mesos_obj:data_obj(),
                    command :: undefined | erl_mesos:command_info() |
                               erl_mesos_obj:data_obj(),
                    container :: undefined | erl_mesos:container_info() |
                                 erl_mesos_obj:data_obj(),
                    data :: undefined | erl_mesos_obj:data_string(),
                    health_check :: undefined | erl_mesos:health_check() |
                                    erl_mesos_obj:data_obj(),
                    labels :: undefined | erl_mesos:labels() |
                              erl_mesos_obj:data_obj(),
                    discovery :: undefined | erl_mesos:discovery_info() |
                                 erl_mesos_obj:data_obj()}).

%% Task status.
-record(task_status, {task_id :: erl_mesos:task_id() | erl_mesos_obj:data_obj(),
                      state :: erl_mesos_obj:data_string(),
                      message :: undefined | erl_mesos_obj:data_string(),
                      source :: undefined | erl_mesos_obj:data_string(),
                      reason :: undefined | erl_mesos_obj:data_string(),
                      data :: undefined | erl_mesos_obj:data_string(),
                      agent_id :: undefined | erl_mesos:agent_id() |
                                  erl_mesos_obj:data_obj(),
                      executor_id :: undefined | erl_mesos:executor_id() |
                                     erl_mesos_obj:data_obj(),
                      timestamp :: undefined | float(),
                      uuid :: undefined | erl_mesos_obj:data_string(),
                      healthy :: undefined | boolean(),
                      labels :: undefined | erl_mesos:labels() |
                                erl_mesos_obj:data_obj(),
                      %% since 0.26.0
                      container_status :: undefined |
                                          erl_mesos:container_status() |
                                          erl_mesos_obj:data_obj()}).

%% Time info.
-record(time_info, {nanoseconds :: non_neg_integer()}).

%% Unavailability.
-record(unavailability, {start :: erl_mesos:time_info() |
                                  erl_mesos_obj:data_obj(),
                         duration :: undefined | erl_mesos:duration_info() |
                                     erl_mesos_obj:data_obj()}).

%% Url.
-record(url, {scheme :: erl_mesos_obj:data_string(),
              address :: erl_mesos:address() | erl_mesos_obj:data_obj(),
              path :: undefined | erl_mesos_obj:data_string(),
              query :: undefined | [erl_mesos:parameter() |
                                    erl_mesos_obj:data_obj()],
              fragment :: undefined | erl_mesos_obj:data_string()}).

%% Value.
-record(value, {type :: erl_mesos:value_type(),
                scalar :: undefined | erl_mesos:value_scalar() |
                          erl_mesos_obj:data_obj(),
                ranges :: undefined | erl_mesos:value_ranges() |
                          erl_mesos_obj:data_obj(),
                set :: undefined | erl_mesos:value_set() |
                       erl_mesos_obj:data_obj(),
                text :: undefined | erl_mesos:value_text() |
                        erl_mesos_obj:data_obj()}).

%% Value type.
-record(value_type, {type :: erl_mesos_obj:data_string()}).

%% Value scalar.
-record(value_scalar, {value :: float()}).

%% Value range.
-record(value_range, {'begin' :: non_neg_integer(),
                      'end' :: non_neg_integer()}).

%% Value ranges.
-record(value_ranges, {range :: [erl_mesos:value_range() |
                                 erl_mesos_obj:data_obj()]}).

%% Value set.
-record(value_set, {item :: erl_mesos_obj:data_string()}).

%% Value text.
-record(value_text, {value :: erl_mesos_obj:data_string()}).

%% Volume.
-record(volume, {mode :: erl_mesos_obj:data_string(),
                 container_path :: erl_mesos_obj:data_string(),
                 host_path :: undefined | erl_mesos_obj:data_string(),
                 image :: undefined | erl_mesos:image() |
                          erl_mesos_obj:data_obj()}).
