-record(framework_info, {user = <<>> :: binary(),
                         name = <<>> :: binary(),
                         id :: framework_id() | erl_mesos_obj:data_obj(),
                         failover_timeout :: float()}).

%% message FrameworkInfo {
%% // Used to determine the Unix user that an executor or task should
%% // be launched as. If the user field is set to an empty string Mesos
%% // will automagically set it to the current user.
%% required string user = 1;
%%
%% // Name of the framework that shows up in the Mesos Web UI.
%% required string name = 2;
%%
%% // Note that 'id' is only available after a framework has
%% // registered, however, it is included here in order to facilitate
%% // scheduler failover (i.e., if it is set then the
%% // MesosSchedulerDriver expects the scheduler is performing
%% // failover).
%% optional FrameworkID id = 3;
%%
%% // The amount of time that the master will wait for the scheduler to
%% // failover before it tears down the framework by killing all its
%% // tasks/executors. This should be non-zero if a framework expects
%% // to reconnect after a failover and not lose its tasks/executors.
%% optional double failover_timeout = 4 [default = 0.0];
%%
%% // If set, framework pid, executor pids and status updates are
%% // checkpointed to disk by the slaves. Checkpointing allows a
%% // restarted slave to reconnect with old executors and recover
%% // status updates, at the cost of disk I/O.
%% optional bool checkpoint = 5 [default = false];
%%
%% // Used to group frameworks for allocation decisions, depending on
%% // the allocation policy being used.
%% optional string role = 6 [default = "*"];
%%
%% // Used to indicate the current host from which the scheduler is
%% // registered in the Mesos Web UI. If set to an empty string Mesos
%% // will automagically set it to the current hostname.
%% optional string hostname = 7;
%%
%% // This field should match the credential's principal the framework
%%   // uses for authentication. This field is used for framework API
%%   // rate limiting and dynamic reservations. It should be set even
%%   // if authentication is not enabled if these features are desired.
%%   optional string principal = 8;
%%
%%   // This field allows a framework to advertise its web UI, so that
%%   // the Mesos web UI can link to it. It is expected to be a full URL,
%%   // for example http://my-scheduler.example.com:8080/.
%%   optional string webui_url = 9;
%%
%%   message Capability {
%%     enum Type {
%%       // Receive offers with revocable resources. See 'Resource'
%%       // message for details.
%%       // TODO(vinod): This is currently a no-op.
%%       REVOCABLE_RESOURCES = 1;
%%     }
%%
%%     required Type type = 1;
%%   }
%%
%%   // This field allows a framework to advertise its set of
%%   // capabilities (e.g., ability to receive offers for revocable
%%   // resources).
%%   repeated Capability capabilities = 10;
%% }

-record(framework_id, {value :: binary()}).

-record(subscribed, {framework_id :: framework_id() | erl_mesos_obj:data_obj(),
                     heartbeat_interval_seconds :: undefined | pos_integer()}).

-type framework_info() :: #framework_info{}.

-type framework_id() :: #framework_id{}.

-type subscribed() :: #subscribed{}.
