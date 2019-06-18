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
{{ task_id | to_var }} = dataproc_operator.DataProcPigOperator(
    task_id={{ task_id | to_python }},
    trigger_rule={{ trigger_rule | to_python }},
    query_uri='%s/%s' % (CONFIG['gcp_uri_prefix'], {{ script_file_name | to_python }}),
    variables={{ params_dict | to_python }},
    dataproc_pig_properties={% include "props.tpl" %},
    cluster_name=CONFIG['dataproc_cluster'],
    gcp_conn_id=CONFIG['gcp_conn_id'],
    region=CONFIG['gcp_region'],
    dataproc_job_id={{ task_id | to_python }},
    params={% include "props.tpl" %},
)
