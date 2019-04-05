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
"""Base mapper - it is a base class for all mappers actions, and logic alike"""
from typing import Set
from xml.etree.ElementTree import Element

import airflow.utils.trigger_rule


class BaseMapper:
    """The Base Mapper class - parent for all mappers."""

    # pylint: disable=unused-argument
    def __init__(
        self,
        oozie_node: Element,
        task_id: str,
        trigger_rule=airflow.utils.trigger_rule.TriggerRule.ALL_SUCCESS,
        params=None,
        **kwargs,
    ):
        if params is None:
            params = {}
        self.params = params
        self.oozie_node = oozie_node
        self.task_id = task_id
        self.trigger_rule = trigger_rule

    def convert_to_text(self) -> str:
        """
        Returns a python operator equivalent string. This will be appended to
        the file.
        """
        raise NotImplementedError("Not Implemented")

    def convert_to_airflow_op(self) -> None:
        """
        Return a corresponding Airflow Operator python object. As of now, it is
        not used, but it could come in handy in the future for extending the
        program.
        """
        raise NotImplementedError("Not Implemented")

    @staticmethod
    def required_imports() -> Set[str]:
        """
        Returns a list of strings that are the import statement that python will
        write to use.

        Ex: returns ['from airflow.operators import bash_operator']
        """
        raise NotImplementedError("Not Implemented")

    def get_task_id(self) -> str:
        """
        Returns the task_id of the operator, this can be handy if any prepare
        statements need to be used or any reordering of operators.
        """
        return self.task_id

    # pylint: disable=unused-argument,no-self-use
    def copy_extra_assets(self, input_directory_path: str, output_directory_path: str) -> None:
        """
        Copies extra assets required by the generated DAG - such as script files, jars etc.
        :param input_directory_path: oozie workflow application directory
        :param output_directory_path: output directory for the generated DAG and assets
        :return: None
        """
        return None
