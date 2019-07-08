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
"""Tests Email mapper"""
import ast
import unittest
from typing import Dict, Any
from xml.etree import ElementTree as ET

from o2a.converter.task import Task
from o2a.mappers import email_mapper
from o2a.o2a_libs.property_utils import PropertySet


class TestEmailMapper(unittest.TestCase):
    def setUp(self) -> None:
        # language=XML
        email_node_str = """
<email>
    <to>to_1_test@gmail.com,to_2_test@gmail.com</to>
    <cc>cc_1_test@example.com,cc_2_test@example.com</cc>
    <bcc>bcc_1_test@gmail.com,bcc_2_test@gmail.com</bcc>
    <subject>Email notifications for ${wf:id()}</subject>
    <body>Hi ${userName}, the wf ${wf:id()} successfully completed. Bye ${userName}</body>
    <content_type>text/plain</content_type>
</email>
        """
        self.email_node = ET.fromstring(email_node_str)

    def test_init(self):
        mapper = self._get_email_mapper(job_properties={}, config={})
        fields: Dict[str, Any] = mapper.__dict__
        self.assertIn("to_addr", fields.keys())
        self.assertIn("cc_addr", fields.keys())
        self.assertIn("bcc_addr", fields.keys())
        self.assertIn("subject", fields.keys())
        self.assertIn("body", fields.keys())

    def test_arguments_are_parsed_correctly(self):
        mapper = self._get_email_mapper(job_properties={"userName": "user"}, config={})
        mapper.on_parse_node()
        self.assertEqual("to_1_test@gmail.com,to_2_test@gmail.com", mapper.to_addr)
        self.assertEqual("cc_1_test@example.com,cc_2_test@example.com", mapper.cc_addr)
        self.assertEqual("bcc_1_test@gmail.com,bcc_2_test@gmail.com", mapper.bcc_addr)
        self.assertEqual("Email notifications for ${wf:id()}", mapper.subject)
        self.assertEqual("Hi user, the wf ${wf:id()} successfully completed. Bye user", mapper.body)

    def test_to_tasks_and_relations(self):
        mapper = self._get_email_mapper(job_properties={"userName": "user"}, config={})
        mapper.on_parse_node()
        tasks, relations = mapper.to_tasks_and_relations()
        self.assertEqual(
            [
                Task(
                    task_id="test_id",
                    template_name="email.tpl",
                    trigger_rule="one_success",
                    template_params={
                        "props": PropertySet(
                            config={}, job_properties={"userName": "user"}, action_node_properties={}
                        ),
                        "to_addr": "to_1_test@gmail.com,to_2_test@gmail.com",
                        "cc_addr": "cc_1_test@example.com,cc_2_test@example.com",
                        "bcc_addr": "bcc_1_test@gmail.com,bcc_2_test@gmail.com",
                        "subject": "Email notifications for ${wf:id()}",
                        "body": "Hi user, the wf ${wf:id()} successfully completed. Bye user",
                    },
                )
            ],
            tasks,
        )
        self.assertEqual(relations, [])

    def test_required_imports(self):
        imps = self._get_email_mapper(job_properties={}, config={}).required_imports()
        imp_str = "\n".join(imps)
        self.assertIsNotNone(ast.parse(imp_str))

    def _get_email_mapper(self, job_properties, config):
        mapper = email_mapper.EmailMapper(
            oozie_node=self.email_node,
            name="test_id",
            dag_name="DAG_NAME_A",
            props=PropertySet(job_properties=job_properties, config=config),
            input_directory_path="/tmp/input-directory-path/",
        )
        return mapper
