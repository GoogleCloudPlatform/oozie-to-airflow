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

from o2a.converter.exceptions import ParseException
from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.prepare_mixin import PrepareMixin
from o2a.utils import xml_utils, el_utils
from o2a.utils.file_archive_extractors import FileExtractor, ArchiveExtractor


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

    application_args: List[str]
    conf: Dict[str, str]
    hdfs_archives: List[str]
    hdfs_files: List[str]
    dataproc_jars: List[str]
    jars: List[str]

    def __init__(
        self,
        oozie_node: ET.Element,
        name: str,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        params: Dict[str, str] = None,
        **kwargs,
    ):
        ActionMapper.__init__(self, oozie_node, name, trigger_rule, **kwargs)
        self.params = params or {}
        self.trigger_rule = trigger_rule
        self.java_class = ""
        self.java_jar = ""
        self.job_name = None
        self.jars = []
        self.properties = {}
        self.application_args = []
        self.file_extractor = FileExtractor(oozie_node=oozie_node, params=self.params)
        self.archive_extractor = ArchiveExtractor(oozie_node=oozie_node, params=self.params)
        self.prepare_command = None
        self.hdfs_files = []
        self.hdfs_archives = []
        self.dataproc_jars = []

    def on_parse_node(self):

        if self.has_prepare:
            self.prepare_command = self.get_prepare_command(oozie_node=self.oozie_node, params=self.params)

        _, self.hdfs_files = self.file_extractor.parse_node()
        _, self.hdfs_archives = self.archive_extractor.parse_node()

        self.java_jar = self._get_or_default(self.oozie_node, SPARK_TAG_JAR, None, params=self.params)
        self.java_class = self._get_or_default(self.oozie_node, SPARK_TAG_CLASS, None, params=self.params)
        if self.java_class and self.java_jar:
            self.dataproc_jars = [self.java_jar]
            self.java_jar = None
        self.job_name = self._get_or_default(self.oozie_node, SPARK_TAG_JOB_NAME, None, params=self.params)

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
            self.application_args.append(el_utils.replace_el_with_var(arg.text, self.params, quote=False))

    @staticmethod
    def _get_or_default(root: ET.Element, tag: str, default: str = None, params: Dict[str, str] = None):
        """
        If a node exists in the oozie_node with the tag specified in tag, it
        will attempt to replace the EL (if it exists) with the corresponding
        variable. If no EL var is found, it just returns text. However, if the
        tag is not found under oozie_node, then return default. If there are
        more than one with the specified tag, it uses the first one found.
        """
        var = xml_utils.find_nodes_by_tag(root, tag)

        if var:
            # Only check the first one
            return el_utils.replace_el_with_var(var[0].text, params=params, quote=False)
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
            task_id=self.name + "_prepare",
            template_name="prepare.tpl",
            template_params=dict(prepare_command=self.prepare_command),
        )
        return [prepare_task, action_task]

    def _get_relations(self):
        """
        Returns the list of Airflow relations that are the result of mapping

        :return: list of relations
        """
        return (
            [Relation(from_task_id=self.name + "_prepare", to_task_id=self.name)]
            if self.has_prepare(self.oozie_node)
            else []
        )

    def to_tasks_and_relations(self):
        tasks = self._get_tasks()
        relations = self._get_relations()
        return tasks, relations

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
