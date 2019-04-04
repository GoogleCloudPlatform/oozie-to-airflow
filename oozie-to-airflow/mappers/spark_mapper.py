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
from typing import Dict, Set

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

import xml.etree.ElementTree as ET

from definitions import TPL_PATH
from mappers.action_mapper import ActionMapper
from utils import xml_utils, el_utils
import jinja2


class SparkMapper(ActionMapper):
    def __init__(
        self,
        oozie_node: ET.Element,
        task_id: str,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        params: Dict[str, str] = None,
        template: str = "spark.tpl",
        **kwargs,
    ):
        ActionMapper.__init__(self, oozie_node, task_id, trigger_rule, **kwargs)
        if params is None:
            params = {}
        self.template = template
        self.params = params
        self.task_id = task_id
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
        self.name = "airflow-spark"
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
            self.delete_paths, self.mkdir_paths = self._parse_prepare_node(prepare_nodes[0])

        # master url, deploy mode,
        self.application = self._test_and_set(oozie_node, "jar", "''", params=self.params, quote=True)
        self.name = self._test_and_set(oozie_node, "name", "'airflow-spark'", params=self.params, quote=True)
        self.java_class = self._test_and_set(oozie_node, "class", None, params=self.params, quote=True)

        config_node = xml_utils.find_nodes_by_tag(oozie_node, "configuration")
        job_xml = xml_utils.find_nodes_by_tag(oozie_node, "job-xml")

        for xml_file in job_xml:
            tree = ET.parse(xml_file.text)
            self.conf = {**self.conf, **self._parse_spark_config(tree.getroot())}

        if config_node:
            self.conf = {**self.conf, **self._parse_spark_config(config_node[0])}

        spark_opts = xml_utils.find_nodes_by_tag(oozie_node, "spark-opts")
        if spark_opts:
            self._update_class_spark_opts(spark_opts[0])

        app_args = xml_utils.find_nodes_by_tag(oozie_node, "arg")
        for arg in app_args:
            self.application_args.append(el_utils.replace_el_with_var(arg.text, self.params, quote=False))

    @staticmethod
    def _test_and_set(
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
        else:
            return default

    @staticmethod
    def _parse_spark_config(config_node: ET.Element) -> Dict[str, str]:
        conf_dict = {}
        for prop in config_node:
            name = prop.find("name").text
            value = prop.find("value").text
            conf_dict[name] = value
        return conf_dict

    def _update_class_spark_opts(self, spark_opts_node: ET.Element):
        """
        Some examples of the spark-opts element:

        '--conf key=value'
        '--conf key1=value1 value2'
        '--conf key1="value1 value2"'
        '--conf key1=value1 key2="value2 value3"'
        '--conf key=value --verbose --properties-file user.properties'
        """

        spark_opts = spark_opts_node.text.split("--")[1:]
        clean_opts = [opt.strip() for opt in spark_opts]
        clean_opts_split = [opt.split(maxsplit=1) for opt in clean_opts]

        if ["verbose"] in clean_opts_split:
            self.__dict__["verbose"] = True
            clean_opts_split.remove(["verbose"])

        for spark_opt in clean_opts_split:
            # Can have multiple "--conf" in spark_opts
            if "conf" == spark_opt[0]:
                # Splits key1=value1 into [key1, value1]
                conf_val = spark_opt[1].split("=", maxsplit=1)
                self.conf[conf_val[0]] = conf_val[1]
            else:
                self.__dict__[spark_opt[0]] = "'" + " ".join(spark_opt[1:]) + "'"

    @staticmethod
    def _parse_prepare_node(prepare_node: ET.Element):
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
        template_loader = jinja2.FileSystemLoader(searchpath=TPL_PATH)
        template_env = jinja2.Environment(loader=template_loader)

        spark_template = template_env.get_template(self.template)
        prepare_template = template_env.get_template("prepare.tpl")

        # If we have found a prepare node, we must reorder nodes.
        if self.delete_paths or self.mkdir_paths:
            prep_text = prepare_template.render(
                task_id=self.task_id,
                trigger_rule=self.trigger_rule,
                delete_paths=self.delete_paths,
                mkdir_paths=self.mkdir_paths,
            )
            # Don't want to change class variable
            op_dict = self.__dict__.copy()
            op_dict["task_id"] = self.task_id + "_reorder"
            op_text = spark_template.render(**op_dict)
            return op_text + prep_text
        else:
            return spark_template.render(**self.__dict__)

    def convert_to_airflow_op(self) -> SparkSubmitOperator:
        """
        Converts the class into a SparkSubmitOperator, this requires
        correct setup of the Airflow connection.

        """
        return SparkSubmitOperator(
            task_id=self.task_id,
            trigger_rule=self.trigger_rule,
            params=self.params,
            # Spark specific
            conn_id="spark_default",
            name=self.name,
            application=self.application,
            conf=self.conf,
            files=self.files,
            py_files=self.py_files,
            jars=self.jars,
            java_class=self.java_class,
            packages=self.packages,
            exclude_packages=self.exclude_packages,
            repositories=self.repositories,
            total_executor_cores=self.total_executor_cores,
            executor_cores=self.executor_cores,
            executor_memory=self.executor_memory,
            driver_memory=self.driver_memory,
            keytab=self.keytab,
            principal=self.principal,
            num_executors=self.num_executors,
            application_args=self.application_args,
            verbose=self.verbose,
            env_vars=self.env_vars,
            driver_classpath=self.driver_classpath,
        )

    @staticmethod
    def required_imports() -> Set[str]:
        # Dummy and Bash are for the potential prepare statement
        return {
            "from airflow.contrib.operators import spark_submit_operator",
            "from airflow.operators import bash_operator",
            "from airflow.operators import dummy_operator",
        }

    def get_task_id(self) -> str:
        # If the prepare node has been parsed then we reconfigure the execution
        # path of Airflow by adding delete/mkdir bash nodes before the actual
        # spark node executes.
        if self.has_prepare():
            return self.task_id + "_reorder"
        else:
            return self.task_id

    def has_prepare(self) -> bool:
        return bool(self.delete_paths or self.mkdir_paths)
