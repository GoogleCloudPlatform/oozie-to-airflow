with models.DAG(
    '{{ dag_name }}',
    schedule_interval={% if schedule_interval %}datetime.timedelta(days={{ schedule_interval }}){% else %}None{% endif %},  # Change to suit your needs
    start_date=dates.days_ago({{ start_days_ago }})  # Change to suit your needs
) as dag:

