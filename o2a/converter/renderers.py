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
"""Classes responsible for generating files based on Workflow"""

# flake8: noqa E402

import logging
import os
import sys
from abc import ABC, abstractmethod
from collections import namedtuple
from pathlib import Path
from typing import Union, Dict, List

from isort.api import sort_file

# pylint: disable=wrong-import-position
# Hack to get rid of INFO level messages printed by blib2to3 when loading grammar
logging.getLogger("blib2to3.pgen2.driver").setLevel(logging.CRITICAL)
# pylint: enable=wrong-import-position


import black

from autoflake import fix_file

from o2a.converter.workflow import Workflow
from o2a.o2a_libs.src.o2a_lib.property_utils import PropertySet
from o2a.utils.el_utils import comma_separated_string_to_list
from o2a.utils.template_utils import render_template

AutoflakeArgs = namedtuple(
    "AutoflakeArgs",
    "remove_all_unused_imports "
    "ignore_init_module_imports "
    "remove_duplicate_keys "
    "remove_unused_variables "
    "in_place imports "
    "expand_star_imports "
    "check",
)


class BaseRenderer(ABC):
    """Base renderer"""

    def __init__(self, output_directory_path, schedule_interval, start_days_ago):
        self.schedule_interval = schedule_interval
        self.start_days_ago = start_days_ago
        self.output_directory_path = output_directory_path

    @abstractmethod
    def create_workflow_file(self, workflow: Workflow, props: PropertySet):
        """
        Create a file with workflow.

        :return: None
        """

    @abstractmethod
    def create_subworkflow_file(self, workflow: Workflow, props: PropertySet):
        """
        Create a file with subworkflow.

        :return: None
        """


class PythonRenderer(BaseRenderer):
    """
    Renderer responsible for generating files in the Python code
    """

    def create_workflow_file(self, workflow: Workflow, props: PropertySet):
        self._create_file(
            output_file_name=os.path.join(self.output_directory_path, workflow.dag_name) + ".py",
            template_name="workflow.tpl",
            workflow=workflow,
            props=props,
        )

    def create_subworkflow_file(self, workflow: Workflow, props: PropertySet):
        self._create_file(
            output_file_name=os.path.join(self.output_directory_path, f"subdag_{workflow.dag_name}.py"),
            template_name="subworkflow.tpl",
            workflow=workflow,
            props=props,
        )

    def _create_file(self, output_file_name, template_name: str, workflow: Workflow, props: PropertySet):
        with open(output_file_name, "w") as file:
            logging.info(f"Saving to file: {output_file_name}")
            dag_content = self._render_content(template_name=template_name, workflow=workflow, props=props)
            file.write(dag_content)

        self._format_with_black(output_file_name)
        self._sort_imports(output_file_name)
        self._remove_unused_imports(output_file_name)

    def _render_content(self, template_name, workflow: Workflow, props: PropertySet):
        """
        Creates text representation of the workflow.
        """
        converted_job_properties: Dict[str, Union[List[str], str]] = {
            key: comma_separated_string_to_list(value) for key, value in props.job_properties.items()
        }
        task_map = {
            task_group.name: [task.task_id for task in task_group.tasks]
            for task_group in workflow.task_groups.values()
        }

        content = render_template(
            template_name=template_name,
            dag_name=workflow.dag_name,
            schedule_interval=self.schedule_interval,
            start_days_ago=self.start_days_ago,
            job_properties=converted_job_properties,
            config=props.config,
            relations=workflow.task_group_relations,
            task_groups=list(workflow.task_groups.values()),
            dependencies=workflow.dependencies,
            task_map=task_map,
        )
        return content

    @staticmethod
    def _remove_unused_imports(output_file_name):
        fix_file(
            output_file_name,
            args=AutoflakeArgs(
                remove_all_unused_imports=True,
                ignore_init_module_imports=False,
                imports=None,
                expand_star_imports=False,
                remove_duplicate_keys=False,
                remove_unused_variables=True,
                in_place=True,
                check=False,
            ),
            standard_out=sys.stdout,
        )

    @staticmethod
    def _format_with_black(output_file_name):
        black.format_file_in_place(
            Path(output_file_name),
            mode=black.FileMode(line_length=110),
            fast=False,
            write_back=black.WriteBack.YES,
        )

    @staticmethod
    def _sort_imports(output_file_name):
        sort_file(filename=output_file_name, ask_to_apply=False)


class DotRenderer(BaseRenderer):
    """
    Renderer responsible for generating files in the DOT code
    """

    def create_workflow_file(self, workflow: Workflow, props: PropertySet):
        output_file_name = os.path.join(self.output_directory_path, workflow.dag_name) + ".dot"
        self._create_file(
            output_file_name=output_file_name, template_name="workflow_dot.tpl", workflow=workflow
        )

    def create_subworkflow_file(self, workflow: Workflow, props: PropertySet):
        output_file_name = os.path.join(self.output_directory_path, f"subdag_{workflow.dag_name}.dot")
        self._create_file(
            output_file_name=output_file_name, template_name="workflow_dot.tpl", workflow=workflow
        )

    def _create_file(self, output_file_name, template_name: str, workflow: Workflow):
        with open(output_file_name, "w") as file:
            logging.info(f"Saving to file: {output_file_name}")
            dag_content = self._render_content(template_name, workflow)
            file.write(dag_content)

    @staticmethod
    def _render_content(template_name, workflow: Workflow):
        """
        Creates text representation of the workflow.
        """
        content = render_template(
            dag_name=workflow.dag_name,
            template_name=template_name,
            relations=workflow.task_group_relations,
            task_groups=list(workflow.task_groups.values()),
        )
        return content
