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

# Copyright 2018 Google LLC
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
"""Tests for the base mapper."""

import unittest

from xml.etree import ElementTree as ET

from o2a.mappers import base_mapper
from airflow.utils.trigger_rule import TriggerRule


class TestBaseMapper(unittest.TestCase):
    def setUp(self):
        # language=XML
        node_str = """
<decision name="decision">
    <switch>
        <case to="task1">${firstNotNull('', '')}</case>
        <case to="task2">True</case>
        <default to="task3" />
    </switch>
</decision>
"""
        self.node = ET.fromstring(node_str)
        self.mapper = base_mapper.BaseMapper(
            oozie_node=self.node, name="test_id", trigger_rule=TriggerRule.DUMMY
        )

    def test_dummy_method(self):
        self.assertEqual(self.mapper.first_task_id, "test_id")
        self.assertEqual(self.mapper.last_task_id, "test_id")
