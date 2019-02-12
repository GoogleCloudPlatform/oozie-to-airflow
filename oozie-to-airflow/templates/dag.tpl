with models.DAG(
    '{{ dag_name }}',
    schedule_interval=datetime.timedelta(days={{ schedule_interval }}),
    start_date=dates.days_ago({{ start_days_ago }})
) as dag:

