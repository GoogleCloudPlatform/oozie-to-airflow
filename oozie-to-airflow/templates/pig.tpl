{{ task_id }} = dataproc_operator.DataProcPigOperator(
    query_uri='{}/{}'.format(PARAMS['gcp_uri_prefix'], '{{ script }}'),
    task_id='{{ task_id }}',
    trigger_rule='{{ trigger_rule }}',
    variables={{ params_dict }},
    dataproc_pig_properties={{ properties }},
    cluster_name=PARAMS['dataproc_cluster'],
    gcp_conn_id=PARAMS['gcp_conn_id'],
    region=PARAMS['gcp_region'],
    dataproc_job_id='{{ task_id }}'
)


