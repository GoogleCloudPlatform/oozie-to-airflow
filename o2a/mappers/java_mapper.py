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
"""Maps Java action into Airflow's DAG"""

from typing import List, Optional, Set
from xml.etree.ElementTree import Element

from o2a.converter.constants import LIB_FOLDER
from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.extensions.prepare_mapper_extension import PrepareMapperExtension
from o2a.o2a_libs.src.o2a_lib.property_utils import PropertySet
from o2a.utils import xml_utils
from o2a.utils.file_archive_extractors import FileExtractor, ArchiveExtractor
from o2a.utils.xml_utils import get_tags_el_array_from_text

TAG_MAIN_CLASS = "main-class"
TAG_JAVA_OPTS = "java-opts"
TAG_JAVA_OPT = "java-opt"
TAG_ARG = "arg"


class JavaMapper(ActionMapper):
    """
    Converts a Java Oozie action node to an Airflow task.
    """

    def __init__(
        self,
        oozie_node: Element,
        name: str,
        dag_name: str,
        props: PropertySet,
        jar_files: List[str],
        **kwargs,
    ):
        ActionMapper.__init__(
            self, oozie_node=oozie_node, dag_name=dag_name, name=name, props=props, **kwargs
        )
        self.file_extractor = FileExtractor(oozie_node=oozie_node, props=self.props)
        self.archive_extractor = ArchiveExtractor(oozie_node=oozie_node, props=self.props)
        self.main_class: Optional[str] = None
        self.java_opts: List[str] = []
        self.args: Optional[List[str]] = None
        self.hdfs_files: Optional[List[str]] = None
        self.hdfs_archives: Optional[List[str]] = None
        self.prepare_extension: PrepareMapperExtension = PrepareMapperExtension(self)
        self.jar_files: List[str] = jar_files if jar_files else []
        self.jar_files_in_hdfs: List[str] = []
        self._get_jar_files_in_hdfs_full_paths()

    def on_parse_node(self):
        super().on_parse_node()
        _, self.hdfs_files = self.file_extractor.parse_node()
        _, self.hdfs_archives = self.archive_extractor.parse_node()
        self._extract_java_data()

    def to_tasks_and_relations(self):
        action_task = Task(
            task_id=self.name,
            template_name="java.tpl",
            template_params=dict(
                props=self.props,
                hadoop_job=dict(
                    args=self.args,
                    jar_file_uris=self.jar_files_in_hdfs,
                    file_uris=self.hdfs_files,
                    archive_uris=self.hdfs_archives,
                    main_class=self.main_class,
                ),
            ),
        )
        tasks = [action_task]
        relations: List[Relation] = []
        prepare_task = self.prepare_extension.get_prepare_task()
        if prepare_task:
            tasks, relations = self.prepend_task(prepare_task, tasks, relations)
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {
            "from airflow.utils import dates",
            "from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator"
        }

    def _get_jar_files_in_hdfs_full_paths(self):
        hdfs_app_prefix = self.props.job_properties["oozie.wf.application.path"]
        for file in self.jar_files:
            self.jar_files_in_hdfs.append(hdfs_app_prefix + "/" + LIB_FOLDER + "/" + file)

    def _extract_java_data(self):
        """Extracts Java node data."""
        root = self.oozie_node
        props = self.props
        if "mapred.child.java.opts" in props.merged:
            self.java_opts.extend(props.merged["mapred.child.java.opts"].split(" "))
        if "mapreduce.map.java.opts" in props.merged:
            self.java_opts.extend(props.merged["mapreduce.map.java.opts"].split(" "))
        self.main_class = xml_utils.get_tag_el_text(root=root, tag=TAG_MAIN_CLASS)
        java_opts_string = xml_utils.get_tag_el_text(root=root, tag=TAG_JAVA_OPTS)
        if java_opts_string:
            self.java_opts.extend(java_opts_string.split(" "))
        else:
            self.java_opts.extend(get_tags_el_array_from_text(root=root, tag=TAG_JAVA_OPT))
        self.args = get_tags_el_array_from_text(root=root, tag=TAG_ARG)
