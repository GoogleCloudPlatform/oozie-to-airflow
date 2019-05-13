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
{{ task_variable_name }} = bash_operator.BashOperator(
    task_id='{{ task_id }}',
    trigger_rule='{{ trigger_rule }}',
    bash_command="gcloud dataproc jobs submit pig --cluster={dataproc_cluster} --region={gcp_region} --execute {pig_command}".format(
        dataproc_cluster=CTX['dataproc_cluster'],
        gcp_region=CTX['gcp_region'],
        pig_command='{{ pig_command }}'.format({% for argument in arguments %}f'{{ argument }}',{% endfor %})
    )
)
