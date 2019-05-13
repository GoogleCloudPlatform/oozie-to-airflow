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

from converter.exceptions import ParseException
from converter.primitives import Relation, Task
from mappers.action_mapper import ActionMapper
from mappers.prepare_mixin import PrepareMixin
from utils import xml_utils, el_utils
from utils.file_archive_extractors import FileExtractor, ArchiveExtractor
from utils.template_utils import render_template


# pylint: disable=too-many-instance-attributes
SPARK_TAG_VALUE = "value"
SPARK_TAG_NAME = "name"
SPARK_TAG_ARGS = "arg"
SPARK_TAG_OPTS = "spark-opts"
SPARK_TAG_CONFIGURATION = "configuration"
SPARK_TAG_JOB_XML = "job-xml"
SPARK_TAG_JOB_NAME = "name"
SPARK_TAG_CLASS = "class"
SPARK_TAG_JAR = "jar"


class SparkMapper(ActionMapper, PrepareMixin):
    """Maps Spark Action"""

    def __init__(
        self,
        oozie_node: ET.Element,
        name: str,
        properties: Dict[str, str],
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        **kwargs,
    ):
        ActionMapper.__init__(
            self, oozie_node=oozie_node, name=name, trigger_rule=trigger_rule, properties=properties, **kwargs
        )
        self.trigger_rule = trigger_rule
        self.java_class = ""
        self.java_jar = ""
        self.job_name = None
        self.jars: List[str] = []

        self.application_args: List[str] = []
        self.file_extractor = FileExtractor(oozie_node=oozie_node, properties=self.properties)
        self.archive_extractor = ArchiveExtractor(oozie_node=oozie_node, properties=self.properties)
        self.prepare_command = None
        self.prepare_arguments = None
        self.hdfs_files: List[str] = []
        self.hdfs_archives: List[str] = []
        self.dataproc_jars: List[str] = []

    def on_parse_node(self):

        if self.has_prepare:
            self.prepare_command, self.prepare_arguments = self.get_prepare_command_with_arguments(
                prepare_node=self.oozie_node, properties=self.properties
            )

        _, self.hdfs_files = self.file_extractor.parse_node()
        _, self.hdfs_archives = self.archive_extractor.parse_node()

        self.java_jar = self._get_or_default(self.oozie_node, SPARK_TAG_JAR)
        self.java_class = self._get_or_default(self.oozie_node, SPARK_TAG_CLASS)
        if self.java_class and self.java_jar:
            self.dataproc_jars = [self.java_jar]
            self.java_jar = None
        self.job_name = self._get_or_default(self.oozie_node, SPARK_TAG_JOB_NAME)

        job_xml_nodes = xml_utils.find_nodes_by_tag(self.oozie_node, SPARK_TAG_JOB_XML)

        for xml_file in job_xml_nodes:
            tree = ET.parse(xml_file.text)
            self.properties.update(self._parse_config_node(tree.getroot()))

        config_nodes = xml_utils.find_nodes_by_tag(self.oozie_node, SPARK_TAG_CONFIGURATION)
        if config_nodes:
            self.properties.update(self._parse_config_node(config_nodes[0]))

        spark_opts = xml_utils.find_nodes_by_tag(self.oozie_node, SPARK_TAG_OPTS)
        if spark_opts:
            self.properties.update(self._parse_spark_opts(spark_opts[0]))

        app_args = xml_utils.find_nodes_by_tag(self.oozie_node, SPARK_TAG_ARGS)
        for arg in app_args:
            self.application_args.append(
                el_utils.convert_el_string_to_fstring(arg.text, properties=self.properties)
            )

    def _get_or_default(self, root: ET.Element, tag: str, default: str = None):
        """
        If a node exists in the oozie_node with the tag specified in tag, it
        will attempt to replace the EL (if it exists) with the corresponding
        variable. If no EL var is found, it just returns text. However, if the
        tag is not found under oozie_node, then return default. If there are
        more than one with the specified tag, it uses the first one found.
        """
        var = xml_utils.find_nodes_by_tag(root, tag)

        if var and var[0].text:
            # Only check the first one
            return el_utils.convert_el_string_to_fstring(var[0].text, properties=self.properties)
        return default

    @staticmethod
    def _parse_config_node(config_node: ET.Element) -> Dict[str, str]:
        conf_dict = {}
        for prop in config_node:
            name_node = prop.find(SPARK_TAG_NAME)
            value_node = prop.find(SPARK_TAG_VALUE)
            if name_node is not None and name_node.text and value_node is not None and value_node.text:
                conf_dict[name_node.text] = value_node.text
        return conf_dict

    @staticmethod
    def _parse_spark_opts(spark_opts_node: ET.Element):
        """
        Some examples of the spark-opts element:
        --conf key1=value
        --conf key2="value1 value2"
        """
        conf = {}
        if spark_opts_node.text:
            spark_opts = spark_opts_node.text.split("--")[1:]
        else:
            raise ParseException("Spark opts node has no text: {}".format(spark_opts_node))
        clean_opts = [opt.strip() for opt in spark_opts]
        clean_opts_split = [opt.split(maxsplit=1) for opt in clean_opts]

        for spark_opt in clean_opts_split:
            # Can have multiple "--conf" in spark_opts
            if spark_opt[0] == "conf":
                key, _, value = spark_opt[1].partition("=")
                # Value is required
                if not value:
                    raise ParseException(
                        f"Incorrect parameter format. Expected format: key=value. Current value: {spark_opt}"
                    )
                # Delete surrounding quotes
                if len(value) > 2 and value[0] in ["'", '"'] and value:
                    value = value[1:-1]
                conf[key] = value

        return conf

    def _get_tasks(self):
        """
        Returns the list of Airflow tasks that are the result of mapping

        :return: list of Airflow tasks
        """
        action_task = Task(
            task_id=self.name,
            template_name="spark.tpl",
            trigger_rule=self.trigger_rule,
            template_params=dict(
                main_jar=self.java_jar,
                main_class=self.java_class,
                arguments=self.application_args,
                archives=self.hdfs_archives,
                files=self.hdfs_files,
                job_name=self.job_name,
                dataproc_spark_properties=self.properties,
                dataproc_spark_jars=self.dataproc_jars,
            ),
        )

        if not self.has_prepare(self.oozie_node):
            return [action_task]

        prepare_task = Task(
            task_id=self.name + "-prepare",
            template_name="prepare.tpl",
            template_params=dict(
                prepare_command=self.prepare_command, prepare_arguments=self.prepare_arguments
            ),
        )
        return [prepare_task, action_task]

    def _get_relations(self):
        """
        Returns the list of Airflow relations that are the result of mapping

        :return: list of relations
        """
        return (
            [Relation(from_task_id=self.name + "-prepare", to_task_id=self.name)]
            if self.has_prepare(self.oozie_node)
            else []
        )

    def convert_to_text(self):
        tasks = self._get_tasks()
        relations = self._get_relations()
        return render_template(template_name="action.tpl", tasks=tasks, relations=relations)

    def required_imports(self) -> Set[str]:
        # Bash are for the potential prepare statement
        return {
            "from airflow.contrib.operators import dataproc_operator",
            "from airflow.operators import bash_operator",
            "from airflow.operators import dummy_operator",
        }

    @property
    def first_task_id(self):
        return self._get_tasks()[0].task_id
