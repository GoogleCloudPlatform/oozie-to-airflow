{{ task_id }}_hook = ssh_hook.SSHHook(
    ssh_conn_id='ssh_default',
    username='{{ user }}',
    remote_host='{{ host }}',
)

{{ task_id }} = ssh_operator.SSHOperator(
    ssh_hook={{ task_id }}_hook,
    task_id='{{ task_id }}',
    trigger_rule='{{ trigger_rule }}',
    params=PARAMS,
    command={{ command }},
)

