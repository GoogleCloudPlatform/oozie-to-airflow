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
"$DAGS_FOLDER/../data/git.sh "
"--cluster=%s "
"--region=%s "
"--git-uri %s "
"--destination-path %s "
{% if git_branch != '' %}"--branch %s " {% endif %}
{% if key_path != '' %}"--key_path %s " {% endif %}
% (CONFIG['dataproc_cluster'], CONFIG['gcp_region'],
shlex.quote({{ git_uri | to_python }}),
shlex.quote({{ destination_path | to_python }}),
{% if git_branch != '' %}shlex.quote({{ git_branch | to_python }}),{% endif %}
{% if key_path != '' %}shlex.quote({{ key_path | to_python }}),{% endif %}
)
