{{ task_id }}_prepare = bash_operator.BashOperator(
    task_id='{{ task_id }}_prepare',
    bash_command='{{ prepare_command }}'
)

{{ task_id }} = bash_operator.BashOperator(
    task_id='{{ task_id }}',
    bash_command="gcloud dataproc jobs submit pig --cluster={0} --region={1} --execute 'sh {{ bash_command }}'"
            .format(PARAMS['dataproc_cluster'], PARAMS['gcp_region'])
)

{{ task_id }}_prepare.set_downstream({{ task_id }})


