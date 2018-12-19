with models.DAG(
    '{{ dag_name }}',
    schedule_interval=datetime.timedelta(days=1),
    start_date=datetime.datetime(2018, 11, 26)
) as dag:

