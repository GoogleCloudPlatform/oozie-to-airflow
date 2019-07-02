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
"""Representation of Airflow tasks"""
from typing import Dict, Any

from airflow.utils.trigger_rule import TriggerRule

from o2a.utils.template_utils import render_template


# This is a container for data, so it does not contain public methods intentionally.
class Task:  # pylint: disable=too-few-public-methods
    """Class for Airflow Task"""

    def __init__(
        self,
        task_id: str,
        template_name: str,
        trigger_rule: str = TriggerRule.ONE_SUCCESS,
        template_params: Dict[str, Any] = None,
    ):
        self.task_id = task_id
        self.template_name = template_name
        self.trigger_rule = trigger_rule
        self.template_params: Dict[str, Any] = template_params or {}

    @property
    def rendered_template(self):
        return render_template(
            template_name=self.template_name,
            task_id=self.task_id,
            trigger_rule=self.trigger_rule,
            **self.template_params,
        )

    def __repr__(self) -> str:
        return (
            f'Task(task_id="{self.task_id}", '
            f'template_name="{self.template_name}", '
            f'trigger_rule="{self.trigger_rule}", '
            f"template_params={self.template_params})"
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False
