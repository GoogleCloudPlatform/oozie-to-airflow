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

import ast
import unittest

from mappers import dummy_mapper
from airflow.utils.trigger_rule import TriggerRule


class TestDummyMapper(unittest.TestCase):
    def test_create_mapper(self):
        mapper = dummy_mapper.DummyMapper(oozie_node=None,
                                          task_id='test_id',
                                          trigger_rule=TriggerRule.DUMMY)
        # make sure everything is getting initialized correctly
        self.assertEqual('test_id', mapper.task_id)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)

    def test_convert_to_text(self):
        mapper = dummy_mapper.DummyMapper(oozie_node=None,
                                          task_id='test_id',
                                          trigger_rule=TriggerRule.DUMMY)
        ast.parse(mapper.convert_to_text())

    def test_required_imports(self):
        imps = dummy_mapper.DummyMapper.required_imports()
        imp_str = '\n'.join(imps)
        ast.parse(imp_str)

