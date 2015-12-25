-module(erl_mesos).

-behaviour(application).

-include("erl_mesos.hrl").

-export([start/0]).

-export([start_scheduler/4, start_scheduler/5, stop_scheduler/1]).

-export([start/2, stop/1]).

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

-type call_reconcile() :: #call_reconcile{}.
-export_type([call_reconcile/0]).

-type call_reconcile_task() :: #call_reconcile_task{}.
-export_type([call_reconcile_task/0]).

-type command_info() :: #command_info{}.
-export_type([command_info/0]).

-type command_info_uri() :: #command_info_uri{}.
-export_type([command_info_uri/0]).

-type command_info_container_info() :: #command_info_container_info{}.
-export_type([command_info_container_info/0]).

-type container_info() :: #container_info{}.
-export_type([container_info/0]).

-type container_info_docker_info() :: #container_info_docker_info{}.
-export_type([container_info_docker_info/0]).

-type container_info_docker_info_port_mapping() ::
    #container_info_docker_info_port_mapping{}.
-export_type([container_info_docker_info_port_mapping/0]).

-type container_info_mesos_info() :: #container_info_mesos_info{}.
-export_type([container_info_mesos_info/0]).

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

-type event_message() :: #event_message{}.
-export_type([event_message/0]).

-type event_failure() :: #event_failure{}.
-export_type([event_failure/0]).

-type event_error() :: #event_error{}.
-export_type([event_error/0]).

-type executor_id() :: #executor_id{}.
-export_type([executor_id/0]).

-type executor_info() :: #executor_info{}.
-export_type([executor_info/0]).

-type filters() :: #filters{}.
-export_type([filters/0]).

-type framework_id() :: #framework_id{}.
-export_type([framework_id/0]).

-type framework_info() :: #framework_info{}.
-export_type([framework_info/0]).

-type framework_info_capabilitie() :: #framework_info_capabilitie{}.
-export_type([framework_info_capabilitie/0]).

-type health_check() :: #health_check{}.
-export_type([health_check/0]).

-type health_check_http() :: #health_check_http{}.
-export_type([health_check_http/0]).

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

%% @doc Starts app with deps.
-spec start() -> ok.
start() ->
    ok = application:ensure_started(asn1),
    ok = application:ensure_started(crypto),
    ok = application:ensure_started(public_key),
    ok = application:ensure_started(ssl),
    ok = application:ensure_started(idna),
    ok = application:ensure_started(hackney),
    ok = application:ensure_started(erl_mesos).

%% @equiv erl_mesos:start_scheduler(Ref, Scheduler, SchedulerOptions, Options,
%%                                  infinity)
-spec start_scheduler(term(), module(), term(),
                      erl_mesos_scheduler:options()) ->
    {ok, pid()} | {error, term()}.
start_scheduler(Ref, Scheduler, SchedulerOptions, Options) ->
    start_scheduler(Ref, Scheduler, SchedulerOptions, Options, infinity).

%% @doc Starts scheduler.
-spec start_scheduler(term(), module(), term(),
                      erl_mesos_scheduler:options(), timeout()) ->
    {ok, pid()} | {error, term()}.
start_scheduler(Ref, Scheduler, SchedulerOptions, Options, Timeout) ->
    erl_mesos_scheduler_manager:start_scheduler(Ref, Scheduler,
                                                SchedulerOptions, Options,
                                                Timeout).

%% @doc Stops scheduler.
-spec stop_scheduler(term())  -> ok | {error, term()}.
stop_scheduler(Ref) ->
    erl_mesos_scheduler_manager:stop_scheduler(Ref).

%% application callback functions.

%% @doc Starts the `erl_mesos_sup' process.
%% @private
-spec start(normal | {takeover, node()} | {failover, node()}, term()) ->
    {ok, pid()} | {error, term()}.
start(_Type, _Args) ->
    erl_mesos_sup:start_link().

%% @private
-spec stop(term()) -> ok.
stop(_State) ->
    ok.
