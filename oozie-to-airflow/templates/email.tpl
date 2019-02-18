{{ task_id }} = email_operator.EmailOperator(
    task_id='{{ task_id }}',
    trigger_rule='{{ trigger_rule }}',
    params=PARAMS,
    to={{ to }},
    subject={{ subject }},
    html_content={{ body }},
    cc={{ cc|default(None, true) }},
    bcc={{ bcc|default(None, true) }},
    mime_subtype='{{ content_type|default('mixed', true) }}',
    files=read_hdfs_files({{ attachment|default([], true) }})
)

