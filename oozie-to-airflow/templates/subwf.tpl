{{ task_id }} = SubDagOperator(
    subdag=sub_dag(dag.dag_id, '{{ task_id }}', dag.start_date, dag.schedule_interval),
    task_id='{{ task_id }}',
)


