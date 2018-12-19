{{ task_id }} = bash_operator.BashOperator(
    bash_command='exit 1',
    task_id='{{ task_id }}',
    trigger_rule='{{ trigger_rule }}',
)


