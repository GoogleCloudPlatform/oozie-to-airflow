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
"""Maps Oozie pig node to Airflow's DAG"""
from typing import Dict, List, Optional, Set
from xml.etree.ElementTree import Element


from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.extensions.prepare_mapper_extension import PrepareMapperExtension
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils.file_archive_extractors import ArchiveExtractor, FileExtractor


# pylint: disable=too-many-instance-attributes
from o2a.utils.param_extractor import extract_param_values_from_action_node
from o2a.utils.xml_utils import get_tag_el_text


class MapReduceMapper(ActionMapper):
    """
    Converts a MapReduce Oozie node to an Airflow task.
    """

    def __init__(self, oozie_node: Element, name: str, dag_name: str, props: PropertySet, **kwargs):
        ActionMapper.__init__(
            self, oozie_node=oozie_node, name=name, dag_name=dag_name, props=props, **kwargs
        )
        self.params_dict: Dict[str, str] = {}
        self.file_extractor = FileExtractor(oozie_node=oozie_node, props=self.props)
        self.archive_extractor = ArchiveExtractor(oozie_node=oozie_node, props=self.props)
        self.name_node: Optional[str] = None
        self.hdfs_files: Optional[List[str]] = None
        self.hdfs_archives: Optional[List[str]] = None
        self.prepare_extension: PrepareMapperExtension = PrepareMapperExtension(self)

    def on_parse_node(self):
        super().on_parse_node()
        self.name_node = get_tag_el_text(self.oozie_node, "name-node", props=self.props)
        self.params_dict = extract_param_values_from_action_node(self.oozie_node, props=self.props)
        _, self.hdfs_files = self.file_extractor.parse_node()
        _, self.hdfs_archives = self.archive_extractor.parse_node()

    def to_tasks_and_relations(self):
        action_task = Task(
            task_id=self.name,
            template_name="mapreduce.tpl",
            template_params=dict(
                props=self.props,
                params_dict=self.params_dict,
                hdfs_files=self.hdfs_files,
                hdfs_archives=self.hdfs_archives,
                action_node_properties=self.props.action_node_properties,
            ),
        )
        tasks = [action_task]
        relations: List[Relation] = []
        prepare_task = self.prepare_extension.get_prepare_task()
        if prepare_task:
            tasks, relations = self.prepend_task(prepare_task, tasks, relations)
        return tasks, relations

    @staticmethod
    def _validate_paths(input_directory_path, output_directory_path):
        if not input_directory_path:
            raise Exception(f"The input_directory_path should be set and is {input_directory_path}")
        if not output_directory_path:
            raise Exception(f"The output_directory_path should be set and is {output_directory_path}")

    def required_imports(self) -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}
