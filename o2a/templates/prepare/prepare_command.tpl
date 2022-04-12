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


{% if mkdir is not none %}
"hadoop fs  -mkdir  %s ;" % (shlex.quote({{ mkdir | to_python }}))

{% if delete is not none %}
+
{% endif %}
{% endif %}


{% if delete is not none %}
"hadoop fs  -rm -r -f  %s ;" % (shlex.quote({{ delete | to_python }}))
{% endif %}
