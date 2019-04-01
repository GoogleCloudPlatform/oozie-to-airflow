    with models.DAG(
        '{0}.{1}'.format(parent_dag_name, child_dag_name),
        schedule_interval=schedule_interval,  # Change to suit your needs
        start_date=start_date  # Change to suit your needs
    ) as dag:

