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
import shutil
from typing import Dict, Set, Optional, List

from xml.etree.ElementTree import Element


from o2a.converter.exceptions import ParseException
from o2a.converter.task import Task
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.extensions.prepare_mapper_extension import PrepareMapperExtension
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils.file_archive_extractors import ArchiveExtractor, FileExtractor


# pylint: disable=too-many-instance-attributes
from o2a.utils.param_extractor import extract_param_values_from_action_node
from o2a.utils.xml_utils import get_tag_el_text


TAG_SCRIPT = "script"
TAG_QUERY = "query"


class HiveMapper(ActionMapper):
    """
    Converts a Hive Oozie node to an Airflow task.
    """

    def __init__(self, oozie_node: Element, name: str, props: PropertySet, **kwargs):
        ActionMapper.__init__(self, oozie_node=oozie_node, name=name, props=props, **kwargs)
        self.variables: Optional[Dict[str, str]] = None
        self.query: Optional[str] = None
        self.script: Optional[str] = None
        self.hdfs_files: Optional[List[str]] = None
        self.hdfs_archives: Optional[List[str]] = None
        self.file_extractor = FileExtractor(oozie_node=oozie_node, props=self.props)
        self.archive_extractor = ArchiveExtractor(oozie_node=oozie_node, props=self.props)
        self.prepare_extension: PrepareMapperExtension = PrepareMapperExtension(self)

    def on_parse_node(self):
        super().on_parse_node()
        self._parse_config()
        self.query = get_tag_el_text(self.oozie_node, TAG_QUERY)
        self.script = get_tag_el_text(self.oozie_node, TAG_SCRIPT)
        if not self.query and not self.script:
            raise ParseException(f"Action Configuration does not include {TAG_SCRIPT} or {TAG_QUERY} element")

        if self.query and self.script:
            raise ParseException(
                f"Action Configuration include {TAG_SCRIPT} and {TAG_QUERY} element. "
                f"Only one can be set at the same time."
            )

        self.variables = extract_param_values_from_action_node(self.oozie_node)
        _, self.hdfs_files = self.file_extractor.parse_node()
        _, self.hdfs_archives = self.archive_extractor.parse_node()

    def to_tasks_and_relations(self):
        action_task = Task(
            task_id=self.name,
            template_name="hive.tpl",
            template_params=dict(
                query=self.query,
                script=self.script,
                props=self.props,
                archives=self.hdfs_archives,
                files=self.hdfs_files,
                variables=self.variables,
            ),
        )
        tasks = [action_task]
        relations = []
        prepare_task = self.prepare_extension.get_prepare_task()
        if prepare_task:
            tasks, relations = self.prepend_task(prepare_task, tasks, relations)

        return tasks, relations

    def copy_extra_assets(self, input_directory_path: str, output_directory_path: str):
        if not self.script:
            return
        source_script_file_path = os.path.join(input_directory_path, self.script)
        destination_script_file_path = os.path.join(output_directory_path, self.script)
        os.makedirs(os.path.dirname(destination_script_file_path), exist_ok=True)
        shutil.copy(source_script_file_path, destination_script_file_path)

    def required_imports(self) -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}
