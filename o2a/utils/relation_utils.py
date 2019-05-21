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
"""Relation utilities"""

from typing import List, Sequence

from o2a.converter.task import Task
from o2a.converter.relation import Relation


def chain(ops: Sequence[Task]) -> List[Relation]:
    """
    Given a number of tasks, builds a relation chain.

    ** Example: **

    :codeblock: pycon
        >>> tasks = [
        ...     Task(task_id="task_a", template_name="task_a.tpl"),
        ...     Task(task_id="task_b", template_name="task_b.tpl"),
        ...     Task(task_id="task_c", template_name="task_c.tpl")
        ... ]
        >>> chain(tasks)
        [Relation(from_task_id='task_a', to_task_id='task_b'), Relation(from_task_id='task_b',
        to_task_id='task_c')]

    :param ops: sequence of tasks
    :return: list of relations
    """
    return [Relation(from_task_id=a.task_id, to_task_id=b.task_id) for a, b in zip(ops, ops[1::])]
