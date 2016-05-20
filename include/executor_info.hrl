-record(executor_info, {data_format :: erl_mesos_data_format:data_format(),
                        api_version :: erl_mesos_scheduler_call:version(),
                        agent_host :: binary(),
                        request_options :: erl_mesos_http:options(),
                        subscribed :: boolean(),
                        framework_id :: erl_mesos:'FrameworkID'(),
                        executor_id :: erl_mesos:'ExecutorID'()}).
