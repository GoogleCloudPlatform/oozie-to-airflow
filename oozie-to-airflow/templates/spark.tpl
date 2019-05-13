{#
  Copyright 2019 Google LLC

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 #}


{{ task_variable_name }} = dataproc_operator.DataProcSparkOperator(
    task_id='{{ task_id }}',
    {% if main_jar %}main_jar=f'{{ main_jar }}',{% endif %}
    {% if main_class %}main_class=f'{{ main_class }}',{% endif %}
    arguments=[{% for argument in arguments %}f'{{argument}}', {% endfor %}],
    {% if archives %}archives=[{% for archive in archives %}f'{{ archive }}', {% endfor %}],{% endif %}
    {% if files %}files=[{% for file in files %}f'{{ file }}', {% endfor %}],{% endif %}
    job_name='{{ job_name }}',
    cluster_name=CTX['dataproc_cluster'],
    {% if dataproc_spark_jars %}dataproc_spark_jars=[{% for jar in dataproc_spark_jars %}f'{{jar}}', {% endfor %}],{% endif %}
    {% if dataproc_spark_properties %}dataproc_spark_properties={{ '{' }}{% for property, value in dataproc_spark_properties.items() %}'{{ property }}': f'{{value}}', {% endfor %}{{ '}' }},{% endif %}
    gcp_conn_id=CTX['gcp_conn_id'],
    region=CTX['gcp_region'],
    trigger_rule='{{ trigger_rule }}',
)
