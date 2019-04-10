# -*- coding: utf-8 -*-
# Copyright 2019 Google LLC
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
"""Maps Spark action to Airflow Dag"""
from typing import Dict, Set, List

import xml.etree.ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule


from mappers.action_mapper import ActionMapper
from utils import xml_utils, el_utils

from utils.template_utils import render_template


# pylint: disable=too-many-instance-attributes
class SparkMapper(ActionMapper):
    """Maps Spark Action"""

    delete_paths: List[str]
    mkdir_paths: List[str]
    application_args: List[str]
    conf: Dict[str, str]

    def __init__(
        self,
        oozie_node: ET.Element,
        name: str,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        params: Dict[str, str] = None,
        template: str = "spark.tpl",
        **kwargs,
    ):
        ActionMapper.__init__(self, oozie_node, name, trigger_rule, **kwargs)
        if params is None:
            params = {}
        self.template = template
        self.params = params
        self.trigger_rule = trigger_rule
        self._parse_oozie_node(oozie_node)

    def _parse_oozie_node(self, oozie_node: ET.Element):
        """
        Property values specified in the configuration element override
        values specified in the job-xml file.
        """
        self.application = ""
        self.conf = {}
        self.conn_id = "spark_default"
        self.files = None
        self.py_files = None
        self.driver_classpath = None
        self.jars = None
        self.java_class = None
        self.packages = None
        self.exclude_packages = None
        self.repositories = None
        self.total_executor_cores = None
        self.executor_cores = None
        self.executor_memory = None
        self.driver_memory = None
        self.keytab = None
        self.principal = None
        self.spark_name = "airflow-spark"
        self.num_executors = None
        self.application_args = []
        self.env_vars = None
        self.verbose = False

        # Prepare nodes
        self.delete_paths = []
        self.mkdir_paths = []

        prepare_nodes = xml_utils.find_nodes_by_tag(oozie_node, "prepare")

        if prepare_nodes:
            # If there exists a prepare node, there will only be one, according
            # to oozie xml schema
            self.delete_paths, self.mkdir_paths = self.parse_prepare_node(prepare_nodes[0])

        # master url, deploy mode,
        self.application = self.test_and_set(oozie_node, "jar", "''", params=self.params, quote=True)
        self.spark_name = self.test_and_set(
            oozie_node, "name", "'airflow-spark'", params=self.params, quote=True
        )
        self.java_class = self.test_and_set(oozie_node, "class", None, params=self.params, quote=True)

        config_node = xml_utils.find_nodes_by_tag(oozie_node, "configuration")
        job_xml = xml_utils.find_nodes_by_tag(oozie_node, "job-xml")

        for xml_file in job_xml:
            tree = ET.parse(xml_file.text)
            self.conf = {**self.conf, **self.parse_spark_config(tree.getroot())}

        if config_node:
            self.conf = {**self.conf, **self.parse_spark_config(config_node[0])}

        spark_opts = xml_utils.find_nodes_by_tag(oozie_node, "spark-opts")
        if spark_opts:
            self.update_class_spark_opts(spark_opts[0])

        app_args = xml_utils.find_nodes_by_tag(oozie_node, "arg")
        for arg in app_args:
            self.application_args.append(el_utils.replace_el_with_var(arg.text, self.params, quote=False))

    @staticmethod
    def test_and_set(
        root: ET.Element, tag: str, default: str = None, params: Dict[str, str] = None, quote: bool = False
    ):
        """
        If a node exists in the oozie_node with the tag specified in tag, it
        will attempt to replace the EL (if it exists) with the corresponding
        variable. If no EL var is found, it just returns text. However, if the
        tag is not found under oozie_node, then return default. If there are
        more than one with the specified tag, it uses the first one found.
        """
        if params is None:
            params = {}
        var = xml_utils.find_nodes_by_tag(root, tag)

        if var:
            # Only check the first one
            return el_utils.replace_el_with_var(var[0].text, params=params, quote=quote)
        return default

    @staticmethod
    def parse_spark_config(config_node: ET.Element) -> Dict[str, str]:
        conf_dict = {}
        for prop in config_node:
            name_node = prop.find("name")
            value_node = prop.find("value")
            if name_node is not None and name_node.text and value_node is not None and value_node.text:
                conf_dict[name_node.text] = value_node.text
        return conf_dict

    def update_class_spark_opts(self, spark_opts_node: ET.Element):
        """
        Some examples of the spark-opts element:

        '--conf key=value'
        '--conf key1=value1 value2'
        '--conf key1="value1 value2"'
        '--conf key1=value1 key2="value2 value3"'
        '--conf key=value --verbose --properties-file user.properties'
        """
        if spark_opts_node.text:
            spark_opts = spark_opts_node.text.split("--")[1:]
        else:
            raise Exception("Spark opts node has no text: {}".format(spark_opts_node))
        clean_opts = [opt.strip() for opt in spark_opts]
        clean_opts_split = [opt.split(maxsplit=1) for opt in clean_opts]

        if ["verbose"] in clean_opts_split:
            self.__dict__["verbose"] = True
            clean_opts_split.remove(["verbose"])

        for spark_opt in clean_opts_split:
            # Can have multiple "--conf" in spark_opts
            if spark_opt[0] == "conf":
                # Splits key1=value1 into [key1, value1]
                conf_val = spark_opt[1].split("=", maxsplit=1)
                self.conf[conf_val[0]] = conf_val[1]
            else:
                self.__dict__[spark_opt[0]] = "'" + " ".join(spark_opt[1:]) + "'"

    @staticmethod
    def parse_prepare_node(prepare_node: ET.Element):
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
        for node in prepare_node:
            node_path = el_utils.convert_el_to_jinja(node.attrib["path"], quote=False)
            if node.tag == "delete":
                delete_paths.append(node_path)
            else:
                mkdir_paths.append(node_path)
        return delete_paths, mkdir_paths

    def convert_to_text(self):
        """Converts subworkflow to text"""
        op_text = render_template(template_name=self.template, task_id=self.name, **self.__dict__)

        # If we have found a prepare node, we must reorder nodes.
        if self.delete_paths or self.mkdir_paths:
            prep_text = render_template(
                template_name="prepare.tpl",
                task_id=self.name + "_reorder",
                trigger_rule=self.trigger_rule,
                delete_paths=self.delete_paths,
                mkdir_paths=self.mkdir_paths,
            )
            return op_text + prep_text
        return op_text

    @staticmethod
    def required_imports() -> Set[str]:
        # Dummy and Bash are for the potential prepare statement
        return {
            "from airflow.contrib.operators import spark_submit_operator",
            "from airflow.operators import bash_operator",
            "from airflow.operators import dummy_operator",
        }

    @property
    def first_task_id(self):
        # If the prepare node has been parsed then we reconfigure the execution
        # path of Airflow by adding delete/mkdir bash nodes before the actual
        # spark node executes.
        if self.has_prepare:
            return "{task_id}_reorder".format(task_id=self.name)
        return self.name

    def has_prepare(self) -> bool:
        return bool(self.delete_paths or self.mkdir_paths)
