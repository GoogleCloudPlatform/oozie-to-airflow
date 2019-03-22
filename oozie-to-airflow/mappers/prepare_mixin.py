# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from utils import xml_utils, el_utils


class PrepareMixin:
    def get_prepare_command(self, oozie_node, params):
        # In BashOperator in Composer we can't read from $DAGS_FOLDER (~/dags) - permission denied.
        # However we can read from ~/data -> /home/airflow/gcs/data.
        # The easiest way to access it is using the $DAGS_FOLDER env variable.
        delete_paths, mkdir_paths = self._parse_prepare_node(oozie_node, params)
        if delete_paths or mkdir_paths:
            delete = " ".join(delete_paths)
            mkdir = " ".join(mkdir_paths)
            return "$DAGS_FOLDER/../data/prepare.sh -c {0} -r {1}{2}{3}".format(
                params["dataproc_cluster"],
                params["gcp_region"],
                ' -d "{}"'.format(delete) if delete else "",
                ' -m "{}"'.format(mkdir) if mkdir else "",
            )
        return ""

    @staticmethod
    def _parse_prepare_node(oozie_node, params):
        """
        <prepare>
            <delete path="[PATH]"/>
            ...
            <mkdir path="[PATH]"/>
            ...
        </prepare>
        """
        delete_paths = []
        mkdir_paths = []
        prepare_nodes = xml_utils.find_nodes_by_tag(oozie_node, "prepare")
        if prepare_nodes:
            # If there exists a prepare node, there will only be one, according
            # to oozie xml schema
            for node in prepare_nodes[0]:
                node_path = el_utils.replace_el_with_var(node.attrib["path"], params=params, quote=False)
                if "//" in node_path:
                    node_path = node_path.split("//", maxsplit=1)[1]  # Removing the hdfs:// or similar part
                node_path = "/" + node_path.split("/", maxsplit=1)[1]  # Removing the 'localhost:8082/' part
                if node.tag == "delete":
                    delete_paths.append(node_path)
                else:
                    mkdir_paths.append(node_path)
        return delete_paths, mkdir_paths
