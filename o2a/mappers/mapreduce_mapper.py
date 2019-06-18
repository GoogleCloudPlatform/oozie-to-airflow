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
from typing import Dict, Set, Tuple, List
from xml.etree.ElementTree import Element

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.extensions.prepare_mapper_extension import PrepareMapperExtension
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils import el_utils, xml_utils
from o2a.utils.file_archive_extractors import ArchiveExtractor, FileExtractor


# pylint: disable=too-many-instance-attributes
class MapReduceMapper(ActionMapper):
    """
    Converts a MapReduce Oozie node to an Airflow task.
    """

    def __init__(
        self,
        oozie_node: Element,
        name: str,
        dag_name: str,
        props: PropertySet,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        **kwargs,
    ):
        ActionMapper.__init__(
            self,
            oozie_node=oozie_node,
            name=name,
            dag_name=dag_name,
            trigger_rule=trigger_rule,
            props=props,
            **kwargs,
        )
        self.params_dict: Dict[str, str] = {}
        self.file_extractor = FileExtractor(oozie_node=oozie_node, props=self.props)
        self.archive_extractor = ArchiveExtractor(oozie_node=oozie_node, props=self.props)
        self.name_node = None
        self.hdfs_files = None
        self.hdfs_archives = None
        self.prepare_extension: PrepareMapperExtension = PrepareMapperExtension(self)

    def on_parse_node(self):
        super().on_parse_node()
        name_node_text = self.oozie_node.find("name-node").text
        self.name_node = el_utils.replace_el_with_var(name_node_text, props=self.props, quote=False)
        self._parse_params()
        _, self.hdfs_files = self.file_extractor.parse_node()
        _, self.hdfs_archives = self.archive_extractor.parse_node()

    def _parse_params(self):
        param_nodes = xml_utils.find_nodes_by_tag(self.oozie_node, "param")
        if param_nodes:
            self.params_dict = {}
            for node in param_nodes:
                param = el_utils.replace_el_with_var(node.text, props=self.props, quote=False)
                key, value = param.split("=", 1)
                self.params_dict[key] = value

    def to_tasks_and_relations(self) -> Tuple[List[Task], List[Relation]]:
        action_task = Task(
            task_id=self.name,
            template_name="mapreduce.tpl",
            trigger_rule=self.trigger_rule,
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
            raise Exception("The input_directory_path should be set and is {}".format(input_directory_path))
        if not output_directory_path:
            raise Exception("The output_directory_path should be set and is {}".format(output_directory_path))

    def required_imports(self) -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}

    @property
    def first_task_id(self) -> str:
        return self.prepare_extension.first_task_id  # type: ignore  # make mypy happy
