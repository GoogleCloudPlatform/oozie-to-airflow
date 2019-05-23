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


{{ task_id | to_var }} = dataproc_operator.DataProcSparkOperator(
    task_id={{ task_id | tojson }},
    trigger_rule={{ trigger_rule | tojson }},
    {% if main_jar %}main_jar={{ main_jar | tojson }},{% endif %}
    {% if main_class %}main_class={{ main_class | tojson }},{% endif %}
    arguments={{ arguments | tojson }},
    {% if archives %}archives={{ archives | tojson }},{% endif %}
    {% if files %}files={{ files | tojson }},{% endif %}
    job_name={{ job_name | tojson }},
    cluster_name=PARAMS['dataproc_cluster'],
    {% if dataproc_spark_jars %}dataproc_spark_jars={{ dataproc_spark_jars | tojson }},{% endif %}
    {% if dataproc_spark_properties %}dataproc_spark_properties={{ dataproc_spark_properties | tojson }},{% endif %}
    gcp_conn_id=PARAMS['gcp_conn_id'],
    region=PARAMS['gcp_region']
)
