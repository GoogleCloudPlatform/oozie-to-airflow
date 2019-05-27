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
from typing import Dict, Set, List, Optional, Tuple

import xml.etree.ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.exceptions import ParseException
from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.prepare_mixin import PrepareMixin
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils import xml_utils, el_utils
from o2a.utils.file_archive_extractors import FileExtractor, ArchiveExtractor


# pylint: disable=too-many-instance-attributes
SPARK_TAG_VALUE = "value"
SPARK_TAG_NAME = "name"
SPARK_TAG_ARGS = "arg"
SPARK_TAG_OPTS = "spark-opts"
SPARK_TAG_JOB_NAME = "name"
SPARK_TAG_CLASS = "class"
SPARK_TAG_JAR = "jar"


class SparkMapper(ActionMapper, PrepareMixin):
    """Maps Spark Action"""

    def __init__(
        self,
        oozie_node: ET.Element,
        name: str,
        property_set: PropertySet,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        **kwargs,
    ):
        ActionMapper.__init__(
            self,
            oozie_node=oozie_node,
            name=name,
            trigger_rule=trigger_rule,
            property_set=property_set,
            **kwargs,
        )
        self.java_class = ""
        self.java_jar = ""
        self.job_name: Optional[str] = None
        self.jars: List[str] = []
        self.application_args: List[str] = []
        self.file_extractor = FileExtractor(oozie_node=oozie_node, property_set=self.property_set)
        self.archive_extractor = ArchiveExtractor(oozie_node=oozie_node, property_set=self.property_set)
        self.hdfs_files: List[str] = []
        self.hdfs_archives: List[str] = []
        self.dataproc_jars: List[str] = []
        self.spark_opts: Dict[str, str] = {}

    def on_parse_node(self):
        super().on_parse_node()
        _, self.hdfs_files = self.file_extractor.parse_node()
        _, self.hdfs_archives = self.archive_extractor.parse_node()

        self.java_jar = self._get_or_default(self.oozie_node, tag=SPARK_TAG_JAR)
        self.java_class = self._get_or_default(self.oozie_node, tag=SPARK_TAG_CLASS)
        if self.java_class and self.java_jar:
            self.dataproc_jars = [self.java_jar]
            self.java_jar = None
        self.job_name = self._get_or_default(self.oozie_node, tag=SPARK_TAG_JOB_NAME)

        spark_opts = xml_utils.find_nodes_by_tag(self.oozie_node, SPARK_TAG_OPTS)
        if spark_opts:
            self.spark_opts.update(self._parse_spark_opts(spark_opts[0]))

        app_args = xml_utils.find_nodes_by_tag(self.oozie_node, SPARK_TAG_ARGS)
        for arg in app_args:
            self.application_args.append(
                el_utils.replace_el_with_var(arg.text, self.property_set, quote=False)
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

        if var and var[0] is not None and var[0].text is not None:
            # Only check the first one
            return el_utils.replace_el_with_var(var[0].text, property_set=self.property_set, quote=False)
        return default

    @staticmethod
    def _parse_spark_opts(spark_opts_node: ET.Element):
        """
        Some examples of the spark-opts element:
        --conf key1=value
        --conf key2="value1 value2"
        """
        conf: Dict[str, str] = {}
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
            # TODO: parse also other options (like --executor-memory 20G --num-executors 50 and many more)
            #  see: https://oozie.apache.org/docs/5.1.0/DG_SparkActionExtension.html#PySpark_with_Spark_Action

        return conf

    def to_tasks_and_relations(self) -> Tuple[List[Task], List[Relation]]:
        action_task = Task(
            task_id=self.name,
            template_name="spark.tpl",
            trigger_rule=self.trigger_rule,
            template_params=dict(
                main_jar=self.java_jar,
                main_class=self.java_class,
                arguments=self.application_args,
                hdfs_archives=self.hdfs_archives,
                hdfs_files=self.hdfs_files,
                job_name=self.job_name,
                dataproc_spark_jars=self.dataproc_jars,
                spark_opts=self.spark_opts,
            ),
        )
        tasks: List[Task] = [action_task]
        relations: List[Relation] = []
        if self.has_prepare(self.oozie_node):
            prepare_task = self.get_prepare_task(
                oozie_node=self.oozie_node,
                name=self.name,
                trigger_rule=self.trigger_rule,
                property_set=self.property_set,
            )
            tasks = [prepare_task, action_task]
            relations = [Relation(from_task_id=prepare_task.task_id, to_task_id=self.name)]
        return tasks, relations

    def required_imports(self) -> Set[str]:
        # Bash are for the potential prepare statement
        return {
            "from airflow.contrib.operators import dataproc_operator",
            "from airflow.operators import bash_operator",
            "from airflow.operators import dummy_operator",
        }

    @property
    def first_task_id(self) -> str:
        return f"{self.name}_prepare" if self.has_prepare(self.oozie_node) else self.name
