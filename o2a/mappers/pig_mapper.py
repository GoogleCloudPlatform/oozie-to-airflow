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
import os
from typing import Dict, List, Set

from xml.etree.ElementTree import Element


from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.extensions.prepare_mapper_extension import PrepareMapperExtension
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils.file_archive_extractors import ArchiveExtractor, FileExtractor
from o2a.utils.param_extractor import extract_param_values_from_action_node
from o2a.utils.xml_utils import get_tag_el_text


TAG_RESOURCE = "resource-manager"
TAG_NAME = "name-node"
TAG_SCRIPT = "script"


# pylint: disable=too-many-instance-attributes
class PigMapper(ActionMapper):
    """
    Converts a Pig Oozie node to an Airflow task.
    """

    def __init__(self, oozie_node: Element, name: str, props: PropertySet, **kwargs):
        ActionMapper.__init__(self, oozie_node=oozie_node, name=name, props=props, **kwargs)
        self.params_dict: Dict[str, str] = {}
        self.file_extractor = FileExtractor(oozie_node=oozie_node, props=self.props)
        self.archive_extractor = ArchiveExtractor(oozie_node=oozie_node, props=self.props)
        self._parse_oozie_node()
        self.prepare_extension: PrepareMapperExtension = PrepareMapperExtension(self)

    def _parse_oozie_node(self):
        self.resource_manager = get_tag_el_text(self.oozie_node, TAG_RESOURCE, self.props)
        self.name_node = get_tag_el_text(self.oozie_node, TAG_NAME, self.props)
        self.script_file_name = get_tag_el_text(self.oozie_node, TAG_SCRIPT, self.props)

        self.params_dict = extract_param_values_from_action_node(self.oozie_node, props=self.props)
        self.files, self.hdfs_files = self.file_extractor.parse_node()
        self.archives, self.hdfs_archives = self.archive_extractor.parse_node()

    def to_tasks_and_relations(self):
        action_task = Task(
            task_id=self.name,
            template_name="pig.tpl",
            template_params=dict(
                props=self.props,
                params_dict=self.params_dict,
                script_file_name=self.script_file_name,
                action_node_properties=self.props.action_node_properties,
            ),
        )
        tasks = [action_task]
        relations: List[Relation] = []
        prepare_task = self.prepare_extension.get_prepare_task()
        if prepare_task:
            tasks, relations = self.prepend_task(prepare_task, tasks, relations)
        return tasks, relations

    def _add_symlinks(self, destination_pig_file):
        destination_pig_file.write("set mapred.create.symlink yes;\n")
        if self.files:
            destination_pig_file.write("set mapred.cache.file {};\n".format(",".join(self.hdfs_files)))
        if self.archives:
            destination_pig_file.write("set mapred.cache.archives {};\n".format(",".join(self.hdfs_archives)))

    def copy_extra_assets(self, input_directory_path: str, output_directory_path: str):
        self._validate_paths(input_directory_path, output_directory_path)
        source_pig_file_path = os.path.join(input_directory_path, self.script_file_name)
        destination_pig_file_path = os.path.join(output_directory_path, self.script_file_name)
        self._copy_pig_script_with_path_injection(destination_pig_file_path, source_pig_file_path)

    def _copy_pig_script_with_path_injection(self, destination_pig_file_path, source_pig_file_path):
        os.makedirs(os.path.dirname(destination_pig_file_path), exist_ok=True)
        with open(destination_pig_file_path, "w") as destination_pig_file:
            with open(source_pig_file_path, "r") as source_pig_file:
                pig_script = source_pig_file.read()
                if self.files or self.archives:
                    self._add_symlinks(destination_pig_file)
                destination_pig_file.write(pig_script)

    @staticmethod
    def _validate_paths(input_directory_path, output_directory_path):
        if not input_directory_path:
            raise Exception(f"The input_directory_path should be set and is {input_directory_path}")
        if not output_directory_path:
            raise Exception(f"The output_directory_path should be set and is {output_directory_path}")

    def required_imports(self) -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}
